defmodule Logflare.Backends.Adaptor.SentryAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Backends.Adaptor.SentryAdaptor
  alias Logflare.Backends.Adaptor.SentryAdaptor.DSN

  @subject SentryAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    :ok
  end

  describe "cast and validate" do
    test "DSN is required" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?
      assert %{dsn: ["can't be blank"]} = errors_on(changeset)
    end

    test "valid DSN passes validation" do
      valid_dsn = "https://abc123@o123456.ingest.sentry.io/123456"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => valid_dsn
        })

      assert changeset.valid?
    end

    test "DSN with secret key passes validation" do
      valid_dsn = "https://public:secret@o123456.ingest.sentry.io/123456"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => valid_dsn
        })

      assert changeset.valid?
    end

    test "invalid DSN format fails validation" do
      invalid_dsn = "not-a-valid-dsn"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => invalid_dsn
        })

      refute changeset.valid?
      assert %{dsn: [error]} = errors_on(changeset)
      assert error =~ "Invalid DSN"
    end

    test "DSN without project ID fails validation" do
      invalid_dsn = "https://abc123@sentry.io/not-a-number"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => invalid_dsn
        })

      refute changeset.valid?
      assert %{dsn: [error]} = errors_on(changeset)
      assert error =~ "Invalid DSN"
    end

    test "DSN with query parameters fails validation" do
      invalid_dsn = "https://abc123@sentry.io/123456?param=value"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => invalid_dsn
        })

      refute changeset.valid?
      assert %{dsn: [error]} = errors_on(changeset)
      assert error =~ "Invalid DSN"
    end
  end

  describe "DSN parsing" do
    test "parse/1 successfully parses valid DSN" do
      dsn = "https://abc123@o123456.ingest.sentry.io/123456"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.original_dsn == dsn
      assert parsed.public_key == "abc123"
      assert parsed.secret_key == nil
      assert parsed.endpoint_uri == "https://o123456.ingest.sentry.io/api/123456/envelope/"
    end

    test "parse/1 handles DSN with secret key" do
      dsn = "https://public:secret@o123456.ingest.sentry.io/123456"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.public_key == "public"
      assert parsed.secret_key == "secret"
    end

    test "parse/1 fails with invalid format" do
      assert {:error, _reason} = DSN.parse("invalid-dsn")
    end

    test "parse/1 fails with query parameters" do
      dsn = "https://abc123@sentry.io/123456?param=value"
      assert {:error, reason} = DSN.parse(dsn)
      assert reason =~ "query parameters"
    end
  end

  describe "transform_config/1" do
    test "converts DSN to webhook configuration" do
      backend = %{
        config: %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"}
      }

      config = @subject.transform_config(backend)

      assert config.url == "https://o123456.ingest.sentry.io/api/123456/envelope/"
      assert config.headers == %{"content-type" => "application/x-sentry-envelope"}
      assert config.http == "http2"
      assert %DSN{} = config._sentry_parsed_dsn
    end

    test "raises error with invalid DSN" do
      backend = %{
        config: %{dsn: "invalid-dsn"}
      }

      assert_raise ArgumentError, ~r/Invalid Sentry DSN/, fn ->
        @subject.transform_config(backend)
      end
    end
  end

  describe "logs ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :sentry,
          sources: [source],
          config: %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"}
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(500)
      [backend: backend, source: source]
    end

    test "sent logs are delivered as Sentry envelope", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        envelope_body = req[:body]
        # Verify it's a Sentry envelope format (3 lines separated by newlines)
        lines = String.split(envelope_body, "\n")
        assert length(lines) == 3

        # Verify envelope structure
        header = Jason.decode!(Enum.at(lines, 0))
        item_header = Jason.decode!(Enum.at(lines, 1))
        item_payload = Jason.decode!(Enum.at(lines, 2))

        assert header["dsn"] == "https://abc123@o123456.ingest.sentry.io/123456"
        assert item_header["type"] == "log"
        assert item_header["content_type"] == "application/vnd.sentry.items.log+json"
        assert is_list(item_payload["items"])

        send(this, ref)
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive ^ref, 2000
    end

    test "log events are properly transformed to Sentry format", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        envelope_body = req[:body]
        lines = String.split(envelope_body, "\n")
        item_payload = Jason.decode!(Enum.at(lines, 2))

        sentry_log = List.first(item_payload["items"])

        # Verify Sentry log structure
        assert Map.has_key?(sentry_log, "timestamp")
        assert Map.has_key?(sentry_log, "level")
        assert Map.has_key?(sentry_log, "body")
        assert Map.has_key?(sentry_log, "attributes")

        # Verify attributes include logflare metadata
        attributes = sentry_log["attributes"]
        assert attributes["sentry.sdk.name"] == "logflare"
        assert attributes["logflare.source.name"] == source.name
        assert attributes["logflare.source.id"] == source.id

        send(this, {ref, sentry_log})
        %Tesla.Env{status: 200, body: ""}
      end)

      le =
        build(:log_event,
          source: source,
          message: "Test log message",
          body: %{
            "timestamp" => System.system_time(:microsecond),
            "message" => "Test log message",
            "level" => "error",
            "user_id" => 123,
            "metadata" => %{"request_id" => "abc-123"}
          }
        )

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, sentry_log}, 2000

      # Verify level mapping
      assert sentry_log["level"] == "error"

      # Verify message
      assert sentry_log["body"] == "Test log message"

      # Verify flattened metadata
      attributes = sentry_log["attributes"]
      assert attributes["user_id"]["value"] == 123
      assert attributes["user_id"]["type"] == "integer"
      assert attributes["metadata.request_id"]["value"] == "abc-123"
      assert attributes["metadata.request_id"]["type"] == "string"
    end

    test "handles log events without explicit level", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        envelope_body = req[:body]
        lines = String.split(envelope_body, "\n")
        item_payload = Jason.decode!(Enum.at(lines, 2))

        sentry_log = List.first(item_payload["items"])

        send(this, {ref, sentry_log})
        %Tesla.Env{status: 200, body: ""}
      end)

      le =
        build(:log_event,
          source: source,
          body: %{
            "timestamp" => System.system_time(:microsecond),
            "message" => "Test message without level"
          }
        )

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, sentry_log}, 2000

      # Should default to "info" level
      assert sentry_log["level"] == "info"
    end

    test "properly maps different log levels", %{source: source} do
      level_mappings = [
        {"debug", "debug"},
        {"info", "info"},
        {"notice", "info"},
        {"warning", "warn"},
        {"warn", "warn"},
        {"error", "error"},
        {"critical", "fatal"},
        {"alert", "fatal"},
        {"emergency", "fatal"},
        {"unknown", "info"}
      ]

      for {input_level, expected_level} <- level_mappings do
        this = self()
        ref = make_ref()

        @client
        |> expect(:send, fn req ->
          envelope_body = req[:body]
          lines = String.split(envelope_body, "\n")
          item_payload = Jason.decode!(Enum.at(lines, 2))

          sentry_log = List.first(item_payload["items"])

          send(this, {ref, sentry_log})
          %Tesla.Env{status: 200, body: ""}
        end)

        le =
          build(:log_event,
            source: source,
            body: %{
              "timestamp" => System.system_time(:microsecond),
              "message" => "Test message",
              "level" => input_level
            }
          )

        assert {:ok, _} = Backends.ingest_logs([le], source)
        assert_receive {^ref, sentry_log}, 2000

        assert sentry_log["level"] == expected_level,
               "Expected #{input_level} to map to #{expected_level}, got #{sentry_log["level"]}"
      end
    end

    test "handles multiple log events in single batch", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        envelope_body = req[:body]
        lines = String.split(envelope_body, "\n")
        item_header = Jason.decode!(Enum.at(lines, 1))
        item_payload = Jason.decode!(Enum.at(lines, 2))

        assert item_header["item_count"] == 3
        assert length(item_payload["items"]) == 3

        send(this, ref)
        %Tesla.Env{status: 200, body: ""}
      end)

      log_events = [
        build(:log_event, source: source, message: "Log 1"),
        build(:log_event, source: source, message: "Log 2"),
        build(:log_event, source: source, message: "Log 3")
      ]

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive ^ref, 2000
    end

    test "handles empty log events list without crashing", %{source: source} do
      backend = %Logflare.Backends.Backend{
        type: :sentry,
        config: %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"}
      }

      transformed_backend = %{backend | config: @subject.transform_config(backend)}

      # Call pre_ingest with empty list - should not crash
      result = @subject.pre_ingest(source, transformed_backend, [])

      # Should return empty list
      assert result == []
    end
  end

  describe "execute_query/3" do
    test "returns not implemented error" do
      result = @subject.execute_query(nil, "SELECT 1", [])
      assert {:error, :not_implemented} = result
    end
  end
end
