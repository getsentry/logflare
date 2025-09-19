defmodule Logflare.Backends.Adaptor.SentryAdaptor do
  @sdk_name "sentry.logflare"
  @sdk_version "0.1.0"
  @sentry_envelope_content_type "application/x-sentry-envelope"

  @moduledoc """
  Sentry adaptor for sending logs to Sentry's logging API.

  This adaptor wraps the WebhookAdaptor to provide specific functionality
  for sending logs to Sentry in the expected envelope format.

  ## Configuration

  The adaptor requires a single configuration parameter:

  - `dsn` - The Sentry DSN string in the format:
    `{PROTOCOL}://{PUBLIC_KEY}:{SECRET_KEY}@{HOST}{PATH}/{PROJECT_ID}`

  ## Example DSN

      https://abc123@o123456.ingest.sentry.io/123456
  """

  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Adaptor.SentryAdaptor.DSN

  @behaviour Logflare.Backends.Adaptor

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    backend = %{backend | config: transform_config(backend)}
    WebhookAdaptor.start_link({source, backend})
  end

  @impl Logflare.Backends.Adaptor
  def transform_config(%_{config: config}) do
    case DSN.parse(config.dsn) do
      {:ok, parsed_dsn} ->
        %{
          url: parsed_dsn.endpoint_uri,
          headers: %{"content-type" => @sentry_envelope_content_type},
          http: "http2",
          _sentry_parsed_dsn: parsed_dsn
        }

      {:error, reason} ->
        raise ArgumentError, "Invalid Sentry DSN: #{reason}"
    end
  end

  @impl Logflare.Backends.Adaptor
  def pre_ingest(_source, _backend, []) do
    # Return empty list if no log events to process
    []
  end

  def pre_ingest(_source, backend, log_events) do
    # Use the already-parsed DSN from the transformed config
    case Map.get(backend.config, :_sentry_parsed_dsn) do
      %DSN{} = parsed_dsn ->
        envelope = build_envelope(log_events, parsed_dsn.original_dsn)

        # Return a single log event containing the envelope as the body
        [
          %Logflare.LogEvent{
            body: envelope,
            source: List.first(log_events).source,
            timestamp: DateTime.utc_now() |> DateTime.to_unix(:microsecond)
          }
        ]

      nil ->
        # If parsed DSN is not available, return empty list to skip sending
        []
    end
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query, _opts), do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{dsn: :string}}
    |> Ecto.Changeset.cast(params, [:dsn])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:dsn])
    |> validate_dsn()
  end

  defp validate_dsn(changeset) do
    case Ecto.Changeset.get_field(changeset, :dsn) do
      nil ->
        changeset

      dsn ->
        case DSN.parse(dsn) do
          {:ok, _parsed_dsn} ->
            changeset

          {:error, reason} ->
            Ecto.Changeset.add_error(changeset, :dsn, "Invalid DSN: #{reason}")
        end
    end
  end

  defp build_envelope(log_events, original_dsn) do
    sentry_logs = Enum.map(log_events, &translate_log_event/1)

    header = %{
      "dsn" => original_dsn,
      "sent_at" => DateTime.utc_now() |> DateTime.to_iso8601(:extended)
    }

    item_header = %{
      "type" => "log",
      "item_count" => length(sentry_logs),
      "content_type" => "application/vnd.sentry.items.log+json"
    }

    item_payload = %{"items" => sentry_logs}

    Enum.join(
      [
        Jason.encode!(header),
        Jason.encode!(item_header),
        Jason.encode!(item_payload)
      ],
      "\n"
    )
  end

  defp translate_log_event(%Logflare.LogEvent{} = le) do
    # Convert microsecond timestamp to seconds (with fractional part)
    timestamp_seconds = le.body["timestamp"] / 1_000_000

    # Extract message from the log event
    message = le.body["message"] || le.body["event_message"] || inspect(le.body)

    # Determine log level
    level =
      case le.body["level"] do
        nil -> "info"
        level when is_binary(level) -> normalize_level(level)
        level when is_atom(level) -> normalize_level(Atom.to_string(level))
        _ -> "info"
      end

    # Build attributes from log event metadata
    attributes = build_attributes(le)

    %{
      "timestamp" => timestamp_seconds,
      "level" => level,
      "body" => message,
      "attributes" => attributes
    }
  end

  defp build_attributes(%Logflare.LogEvent{} = le) do
    base_attrs = %{
      "sentry.sdk.name" => @sdk_name,
      "sentry.sdk.version" => @sdk_version,
      "logflare.source.name" => le.source.name,
      "logflare.source.id" => le.source.id
    }

    # Add all metadata from the log event body, excluding standard fields
    event_attrs =
      le.body
      |> Map.drop(["timestamp", "message", "event_message", "level"])
      |> flatten_map()
      |> Map.new(fn {k, v} -> {k, to_sentry_value(v)} end)

    Map.merge(base_attrs, event_attrs)
  end

  defp normalize_level(level_string) do
    case String.downcase(level_string) do
      "debug" -> "debug"
      "info" -> "info"
      "notice" -> "info"
      "warning" -> "warn"
      "warn" -> "warn"
      "error" -> "error"
      "critical" -> "fatal"
      "alert" -> "fatal"
      "emergency" -> "fatal"
      _ -> "info"
    end
  end

  defp to_sentry_value(value) do
    case value do
      v when is_binary(v) -> %{"value" => v, "type" => "string"}
      v when is_boolean(v) -> %{"value" => v, "type" => "boolean"}
      v when is_integer(v) -> %{"value" => v, "type" => "integer"}
      v when is_float(v) -> %{"value" => v, "type" => "double"}
      v -> %{"value" => to_string(v), "type" => "string"}
    end
  end

  defp flatten_map(map), do: flatten_map(map, nil, %{})

  defp flatten_map(%{} = map, parent, acc) do
    Enum.reduce(map, acc, fn {k, v}, acc2 ->
      key =
        [parent, k]
        |> Enum.reject(&is_nil/1)
        |> Enum.map(&to_string/1)
        |> Enum.join(".")

      flatten_map(v, key, acc2)
    end)
  end

  defp flatten_map(value, key, acc), do: Map.put(acc, key, value)
end
