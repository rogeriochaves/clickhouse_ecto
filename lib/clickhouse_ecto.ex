defmodule ClickhouseEcto do
  @moduledoc """
  Adapter module for ClickHouse.

  It uses `clickhousex` for communication with the database.any()

  ## Features

  ## Options

  ### Connection options

  ### Storage options

  ### After connect callback

  """

  # Inherit all behaviour from Ecto.Adapters.SQL
  use Ecto.Adapters.SQL,
    driver: :clickhousex,
    migration_lock: "FOR UPDATE"

  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  alias ClickhouseEcto.Storage

  import ClickhouseEcto.Type, only: [encode: 2, decode: 2]

  @impl true
  def dumpers({:embed, _} = type, _), do: [&Ecto.Adapters.SQL.dump_embed(type, &1)]
  def dumpers(:binary_id, _type), do: []
  def dumpers(:uuid, _type), do: []
  def dumpers(ecto_type, type), do: [type, &encode(&1, ecto_type)]

  # def autogenerate(:binary_id), do: Ecto.UUID.generate()
  # def autogenerate(type), do: super(type)

  ## Storage API
  @impl true
  def storage_up(opts), do: Storage.storage_up(opts)

  @impl true
  def storage_down(opts), do: Storage.storage_down(opts)

  def loaders({:embed, _} = type, _), do: [&Ecto.Adapters.SQL.load_embed(type, &1)]
  def loaders(ecto_type, type), do: [&decode(&1, ecto_type), type]

  @doc """
  ClickHouse doesn't have full-fledged transactions
  """
  @impl true
  def supports_ddl_transaction? do
    false
  end

  ## Structure API (not implemented yet)

  @impl true
  def structure_dump(_default, _config) do
    {:error, :not_implemented}
  end

  @impl true
  def structure_load(_default, _config) do
    {:error, :not_implemented}
  end
end
