defmodule ClickhouseEcto.Connection do
  alias Clickhousex.Query
  alias ClickhouseEcto.Query, as: SQL

  @behaviour Ecto.Adapters.SQL.Connection

  @typedoc "The prepared query which is an SQL command"
  @type prepared :: String.t()

  @typedoc "The cache query which is a DBConnection Query"
  @type cached :: map

  @impl true
  def child_spec(opts) do
    Clickhousex.child_spec(opts)
  end

  @doc """
  Prepares and executes the given query with `DBConnection`.
  """
  @impl true
  def prepare_execute(conn, name, statement, params, options) do
    query = %Query{name: name, statement: statement}

    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, query, result} ->
        {:ok, %{query | statement: statement}, process_rows(result, options)}

      {:error, %Clickhousex.Error{}} = error ->
        if is_no_data_found_bug?(error, statement) do
          {:ok, %Query{name: "", statement: statement}, %{num_rows: 0, rows: []}}
        else
          error
        end

      {:error, error} ->
        raise error
    end
  end

  @doc """
  Executes the given prepared query with `DBConnection`.
  """
  @impl true
  def execute(conn, %Query{} = query, params, options) do
    case DBConnection.prepare_execute(conn, query, params, options) do
      {:ok, _query, result} ->
        {:ok, process_rows(result, options)}

      {:error, %Clickhousex.Error{}} = error ->
        if is_no_data_found_bug?(error, query.statement) do
          {:ok, %{num_rows: 0, rows: []}}
        else
          error
        end

      {:error, error} ->
        raise error
    end
  end

  def execute(conn, statement, params, options) do
    execute(conn, %Query{name: "", statement: statement}, params, options)
  end

  @impl true
  def query(conn, statement, params, options) do
    Clickhousex.query(conn, statement, params, options)
    |> case do
      {:ok, _, result} -> {:ok, result}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Returns a stream that prepares and executes the given query with `DBConnection`.
  """
  @impl true
  def stream(_conn, _statement, _params, _options) do
    raise("stream/4 not implemented")
  end

  @impl true
  def to_constraints(_error), do: []

  ## Queries

  @impl true
  def all(query) do
    SQL.all(query)
  end

  @impl true
  def update_all(_query) do
    raise "UPDATE is not supported"
  end

  @impl true
  def delete_all(_query) do
    raise "DELETE is not supported"
  end

  @impl true
  def insert(prefix, table, header, rows, on_conflict, returning) do
    SQL.insert(prefix, table, header, rows, on_conflict, returning)
  end

  @impl true
  def update(_prefix, _table, _fields, _filters, _returning) do
    raise "UPDATE is not supported"
  end

  @impl true
  def delete(_prefix, _table, _filters, _returning) do
    raise "DELETE is not supported"
  end

  ## DDL

  @impl true
  def execute_ddl(command), do: ClickhouseEcto.Migration.execute_ddl(command)

  @impl true
  def ddl_logs(_result), do: []

  @impl true
  def table_exists_query(_table) do
    raise "table_exists_query/1 not implemented"
  end

  ## Helpers

  defp process_rows(result, options) do
    decoder = options[:decode_mapper] || fn x -> x end

    Map.update!(result, :rows, fn row ->
      unless is_nil(row), do: Enum.map(row, decoder)
    end)
  end

  defp is_no_data_found_bug?({:error, error}, statement) do
    is_dml =
      statement
      |> IO.iodata_to_binary()
      |> (fn string ->
            String.starts_with?(string, "INSERT") || String.starts_with?(string, "DELETE") ||
              String.starts_with?(string, "UPDATE")
          end).()

    is_dml and error.message =~ "No SQL-driver information available."
  end
end
