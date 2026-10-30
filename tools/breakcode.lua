-- Let long inline code wrap: insert \allowbreak after / . _ , = : in LaTeX output.
function Code(el)
  if not FORMAT:match("latex") or #el.text < 16 then return nil end
  local out = {}
  for piece in el.text:gmatch("[^/%._,=:]*[/%._,=:]?") do
    if piece ~= "" then
      out[#out + 1] = pandoc.Code(piece, el.attr)
      out[#out + 1] = pandoc.RawInline("latex", "\\allowbreak{}")
    end
  end
  return out
end
