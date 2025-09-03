local json = require "json"

function onRecord(record)
  local row = record.row

  if row == nil then
    return nil
  end

  local columns = peerdb.RowColumns(row)
  local result = {}

  for i = 1, #columns do
    local column = columns[i]
    local value = row[column]
    local kind = peerdb.RowColumnKind(row, column)

    -- Auto-detect both JSON and JSONB fields and apply json.raw to avoid double encoding in json.encode
    if (kind == "Json" or kind == "Jsonb" or kind == "jsonb" or kind == "json") and value and value ~= "" then
      result[column] = json.raw(value)
    else
      result[column] = value
    end
  end

  return {
    key = tostring(row.id),
    value = json.encode(result)
  }
end