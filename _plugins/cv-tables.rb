# frozen_string_literal: true

# Kramdown's GFM pipe tables require a header row even when the Markdown source
# has none, so the CV uses `| | |` / `|:-|:-|` for unlabelled tables. That
# renders an empty <thead> whose cells hold only a no-break space: a blank band
# above the table and empty column headers announced to screen readers.
#
# Drop those all-blank header rows on the Markdown CV page only. Tables that do
# carry real header labels are left untouched.
Jekyll::Hooks.register %i[pages documents], :post_render do |doc|
  next unless doc.output&.include?('<div class="cv-page">')

  doc.output = doc.output.gsub(
    %r{<thead>\s*<tr>(?:\s*<th[^>]*>(?:\s|\u00A0)*</th>)+\s*</tr>\s*</thead>},
    ""
  )
end
