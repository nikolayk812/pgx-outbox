local json = require "json"

function onRecord(record)
  local row = record.row

  if row == nil then
    return nil
  end

  -- Manual construction
  local result = {
    id = row.id,
    created_at = row.created_at
  }

  -- Add JSONB fields with json.raw
  if row.metadata and row.metadata ~= "" then
    result.metadata = json.raw(row.metadata)
  end

  if row.payload and row.payload ~= "" then
    result.payload = json.raw(row.payload)
  end

  return {
    key = tostring(row.id),
    value = json.encode(result)
  }
end