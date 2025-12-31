import type { GraphResponse } from "./types"

const API_BASE = import.meta.env.VITE_API_BASE ?? ""
const apiUrl = (path: string) => `${API_BASE}${path}`

export async function searchItems(query: string, limit?: number) {
  const params = new URLSearchParams({ q: query })
  if (limit) {
    params.set("limit", String(limit))
  }
  const res = await fetch(apiUrl(`/api/search/items?${params.toString()}`))
  return res.json()
}

export async function searchFluids(query: string, limit?: number) {
  const params = new URLSearchParams({ q: query })
  if (limit) {
    params.set("limit", String(limit))
  }
  const res = await fetch(apiUrl(`/api/search/fluids?${params.toString()}`))
  return res.json()
}

export async function fetchGraph(payload: unknown): Promise<GraphResponse> {
  const res = await fetch(apiUrl("/api/graph"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  })
  if (!res.ok) {
    throw new Error("Graph request failed")
  }
  return res.json()
}

export async function fetchRecipesByOutput(params: {
  output_type: "item" | "fluid"
  item_id?: string
  meta?: number
  fluid_id?: string
  machine_id?: string
  limit?: number
}) {
  const query = new URLSearchParams({ output_type: params.output_type })
  if (params.item_id) query.set("item_id", params.item_id)
  if (params.meta !== undefined) query.set("meta", String(params.meta))
  if (params.fluid_id) query.set("fluid_id", params.fluid_id)
  if (params.machine_id) query.set("machine_id", params.machine_id)
  if (params.limit) query.set("limit", String(params.limit))
  const res = await fetch(apiUrl(`/api/recipes/by-output?${query.toString()}`))
  return res.json()
}

export async function fetchRecipesByInput(params: {
  input_type: "item" | "fluid"
  item_id?: string
  meta?: number
  fluid_id?: string
  machine_id?: string
  limit?: number
  downstream_type?: "item" | "fluid"
  downstream_item_id?: string
  downstream_meta?: number
  downstream_fluid_id?: string
  max_depth?: number
}) {
  const query = new URLSearchParams({ input_type: params.input_type })
  if (params.item_id) query.set("item_id", params.item_id)
  if (params.meta !== undefined) query.set("meta", String(params.meta))
  if (params.fluid_id) query.set("fluid_id", params.fluid_id)
  if (params.machine_id) query.set("machine_id", params.machine_id)
  if (params.limit) query.set("limit", String(params.limit))
  if (params.downstream_type) query.set("downstream_type", params.downstream_type)
  if (params.downstream_item_id) query.set("downstream_item_id", params.downstream_item_id)
  if (params.downstream_meta !== undefined) query.set("downstream_meta", String(params.downstream_meta))
  if (params.downstream_fluid_id) query.set("downstream_fluid_id", params.downstream_fluid_id)
  if (params.max_depth) query.set("max_depth", String(params.max_depth))
  const res = await fetch(apiUrl(`/api/recipes/by-input?${query.toString()}`))
  return res.json()
}

export async function fetchMachinesByOutput(params: {
  output_type: "item" | "fluid"
  item_id?: string
  meta?: number
  fluid_id?: string
  limit?: number
}) {
  const query = new URLSearchParams({ output_type: params.output_type })
  if (params.item_id) query.set("item_id", params.item_id)
  if (params.meta !== undefined) query.set("meta", String(params.meta))
  if (params.fluid_id) query.set("fluid_id", params.fluid_id)
  if (params.limit) query.set("limit", String(params.limit))
  const res = await fetch(apiUrl(`/api/machines/by-output?${query.toString()}`))
  return res.json()
}
