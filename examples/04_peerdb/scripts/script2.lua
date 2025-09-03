local json = require "json"

function onRecord(record)
  local row = record.row

  if row == nil then
    return nil
  end

  -- Get all column names
  local columns = peerdb.RowColumns(row)
  local result = {}

  for i = 1, #columns do
    local column = columns[i]
    local value = row[column]

    -- field names are hardcoded
    if (column == "metadata" or column == "payload") and value and value ~= "" then
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