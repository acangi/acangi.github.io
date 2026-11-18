# frozen_string_literal: true

# Site-wide link policy for rendered pages and documents.
#
# Every <a>/<area> with a non-empty href opens in a new tab and carries both
# security tokens:
#
#     target="_blank" rel="noopener noreferrer"
#
# This is deliberately literal: same-document fragments (the skip link,
# table-of-contents links), mailto:/tel:/sms:, data:/blob: URLs, and links with
# the `download` attribute are included, not just external or path-navigating
# links.  The URL scheme and `download` attribute are preserved, so the
# browser still hands mailto:/tel: to the protocol handler and download links
# to the download manager; `target` does not replace those browser controls.
#
# The one exception is javascript: pseudo-links.  They are UI controls rather
# than navigations, so they are left byte-for-byte unchanged (including any
# target/rel a page author wrote).
#
# Server-rendered markup is rewritten here, after the layout has been rendered
# and after jekyll-link-attributes has handled external links.  A small runtime
# is injected into each HTML document as well, because some anchors are created
# by client-side JavaScript (Leaflet attribution, publication badges, ...) and
# never pass through Jekyll's renderer.
#
# Existing rel tokens such as jekyll-link-attributes' "external nofollow" are
# preserved, and rel="opener" is removed from every rewritten link.
#
# The transformer scans markup left to right and only rewrites start tags for
# real <a>/<area> elements.  Attribute values are treated as opaque, so text
# such as <div data-html="<a href='/x'>n</a>"> is preserved byte-for-byte;
# only the tag currently being scanned can be a link.  Quoted attribute values
# may contain ">" and the scanner skips <script>, <style>, <textarea>, <title>
# and <xmp> elements and comments as raw text.  It edits attribute spans in
# place, so the rest of the document keeps its original bytes instead of being
# re-serialized by a parser.
module Jekyll
  module AllLinksInNewTabs
    SECURITY_TOKENS = %w[noopener noreferrer].freeze
    RUNTIME_MARKER = "data-all-links-new-tab"
    PSEUDO_SCHEME = %r{\Ajavascript:}i
    LINK_TAGS = %w[a area].freeze
    # Elements whose content is text, not markup, for HTML parsers.
    RAW_TEXT_ELEMENTS = %w[script style textarea title xmp].freeze
    # Character references used only to recognise an encoded javascript:
    # scheme.  Browsers resolve these in attribute values before parsing the
    # URL, so "&#106;avascript&#58;" is a javascript: URL too.
    NUMERIC_CHAR_REF = /&#(?:(\d{1,7})|[xX](\h{1,6}));?/
    NAMED_CHAR_REF = /&(colon|Tab|NewLine);/
    NAMED_CHAR_REFS = { "colon" => ":", "Tab" => "\t", "NewLine" => "\n" }.freeze

    RUNTIME_JS = <<~'JS'
      (function () {
        "use strict";
        var JAVASCRIPT = /^javascript:/i;
        var TOKENS = ["noopener", "noreferrer"];
        function isPseudo(value) {
          return JAVASCRIPT.test(
            value.replace(/[\t\n\r]/g, "").replace(/^[\u0000-\u0020]+/, "")
          );
        }
        function secure(element) {
          if (!element) return;
          var tag = (element.tagName || "").toUpperCase();
          if (tag !== "A" && tag !== "AREA") return;
          var href = element.getAttribute("href");
          if (href === null) return;
          var value = href.trim();
          // The DOM already resolves character references in href, so only
          // ASCII tab/newline (which URL parsing drops) needs normalizing.
          if (value === "" || isPseudo(value)) return;
          var target = element.getAttribute("target");
          if (target !== "_blank") {
            element.setAttribute("target", "_blank");
            target = "_blank";
          }
          var tokens = (element.getAttribute("rel") || "").split(/\s+/);
          var kept = [];
          var i;
          for (i = 0; i < tokens.length; i += 1) {
            if (tokens[i] && tokens[i].toLowerCase() !== "opener") kept.push(tokens[i]);
          }
          for (i = 0; i < TOKENS.length; i += 1) {
            var found = false;
            for (var j = 0; j < kept.length; j += 1) {
              if (kept[j].toLowerCase() === TOKENS[i]) { found = true; break; }
            }
            if (!found) kept.push(TOKENS[i]);
          }
          element.setAttribute("rel", kept.join(" "));
        }
        function scan(root) {
          if (!root) return;
          if (root.nodeType === 1) secure(root);
          if (typeof root.querySelectorAll !== "function") return;
          var links = root.querySelectorAll("a[href], area[href]");
          for (var i = 0; i < links.length; i += 1) secure(links[i]);
        }
        if (document.readyState === "loading") {
          document.addEventListener("DOMContentLoaded", function () { scan(document); });
        } else {
          scan(document);
        }
        if (window.MutationObserver) {
          new window.MutationObserver(function (mutations) {
            for (var i = 0; i < mutations.length; i += 1) {
              var mutation = mutations[i];
              if (mutation.type === "attributes") {
                secure(mutation.target);
              } else {
                for (var j = 0; j < mutation.addedNodes.length; j += 1) {
                  scan(mutation.addedNodes[j]);
                }
              }
            }
          }).observe(document.documentElement, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ["href", "target"]
          });
        }
      })();
    JS

    RUNTIME_SCRIPT = "<script #{RUNTIME_MARKER}>\n#{RUNTIME_JS}</script>"

    class << self
      def transform(document)
        return document unless document.is_a?(String) && !document.empty?

        output = rewrite_tags(document)
        inject_runtime(output)
      end

      private

      # Walk the document as a tokenizer: text runs are copied, protected
      # regions are copied verbatim, and only a start tag whose own name is a
      # link is rewritten.  A "<a" inside another tag's quoted attribute is
      # therefore never seen as markup.
      def rewrite_tags(document)
        output = +""
        index = 0
        length = document.length

        while index < length
          opening = document.index("<", index)
          if opening.nil?
            output << document[index, length - index]
            break
          end

          output << document[index, opening - index] if opening > index

          protected_stop = protected_region_end(document, opening)
          if protected_stop
            output << document[opening, protected_stop - opening]
            index = protected_stop
            next
          end

          name_match = /\G<([a-zA-Z][^\s\/>]*)/.match(document, opening)
          if name_match.nil?
            # End tags, doctypes and processing instructions are copied as a
            # unit: their text can never contain a real anchor start tag.
            if document[opening + 1] == "/"
              closing_match = /\G<\/([a-zA-Z][^\s\/>]*)/.match(document, opening)
              if closing_match
                finish = tag_end(document, opening)
                output << document[opening, finish + 1 - opening]
                index = finish + 1
                next
              end
            elsif document[opening + 1] == "!" || document[opening + 1] == "?"
              close = document.index(">", opening + 2)
              finish = close || length - 1
              output << document[opening, finish + 1 - opening]
              index = finish + 1
              next
            end

            output << "<"
            index = opening + 1
            next
          end

          name = name_match[1]
          finish = tag_end(document, opening)
          if LINK_TAGS.include?(name.downcase) && document[finish] == ">"
            attributes_start = opening + 1 + name.length
            attributes = document[attributes_start, finish - attributes_start]
            output << "<#{name}#{rewrite_attributes(attributes)}>"
          else
            output << document[opening, finish + 1 - opening]
          end
          index = finish + 1
        end

        output
      end

      # Exclusive end of a comment or raw-text element starting at `index`, or
      # nil when the "<" begins an ordinary tag or literal text.
      def protected_region_end(document, index)
        if document[index, 4] == "<!--"
          close = document.index("-->", index + 4)
          return close ? close + 3 : document.length
        end

        if document[index, 9] == "<![CDATA["
          close = document.index("]]>", index + 9)
          return close ? close + 3 : document.length
        end

        match = /\G<([a-zA-Z][^\s\/>]*)/.match(document, index)
        return nil unless match

        name = match[1].downcase
        return nil unless RAW_TEXT_ELEMENTS.include?(name)

        tag_finish = tag_end(document, index)
        close = %r{</#{Regexp.escape(name)}\s*>}i.match(document, tag_finish + 1)
        # An unclosed raw-text element is broken markup; scanning its start tag
        # normally is safer than swallowing the rest of the document.
        close && close.end(0)
      end

      # Index of the ">" that ends the tag starting at `start`, skipping quoted
      # attribute values (which may contain ">").  Returns the last index when
      # the tag is unterminated.
      def tag_end(document, start)
        index = start + 1
        length = document.length
        quote = nil

        while index < length
          character = document[index]
          if quote
            quote = nil if character == quote
          elsif character == '"' || character == "'"
            quote = character
          elsif character == ">"
            return index
          end
          index += 1
        end

        length - 1
      end

      def rewrite_attributes(attributes)
        tokens = scan_attributes(attributes)
        href_token = tokens.find { |token| token[:name].casecmp?("href") }
        target_token = tokens.find { |token| token[:name].casecmp?("target") }
        rel_token = tokens.find { |token| token[:name].casecmp?("rel") }

        href = href_token && href_token[:value].to_s.strip
        # Empty hrefs and javascript: pseudo-links are not navigations.
        return attributes if href.to_s.empty? || pseudo_href?(href)

        target = target_token && target_token[:value].to_s.strip
        blank = target && target.casecmp?("_blank")
        needs_target = !blank
        needs_rel = true

        edits = []
        additions = []

        if needs_target
          if target_token.nil?
            additions << 'target="_blank"'
          elsif target_token[:value].nil?
            edits << [target_token[:start], target_token[:finish], 'target="_blank"']
          else
            edits << [target_token[:value_start], target_token[:value_finish], "_blank"]
          end
        end

        if needs_rel
          new_rel = merged_rel(rel_token && rel_token[:value])
          if rel_token.nil?
            additions << %(rel="#{new_rel}")
          elsif rel_token[:value].nil?
            edits << [rel_token[:start], rel_token[:finish], %(rel="#{new_rel}")]
          elsif rel_token[:quote]
            edits << [rel_token[:value_start], rel_token[:value_finish], new_rel]
          else
            # A multi-token rel needs quotes; unquoted values cannot hold spaces.
            edits << [rel_token[:value_start], rel_token[:value_finish], %("#{new_rel}")]
          end
        end

        # Apply from right to left so earlier byte offsets stay valid.
        edits.sort_by { |start, _finish, _replacement| -start }.each do |start, finish, replacement|
          attributes[start...finish] = replacement
        end

        append_attributes(attributes, additions)
      end

      def merged_rel(value)
        existing = value.to_s.split(/\s+/).reject(&:empty?)
        # rel=opener explicitly re-enables the relationship we are preventing.
        existing.reject! { |token| token.casecmp?("opener") }
        SECURITY_TOKENS.each do |required|
          next if existing.any? { |token| token.casecmp?(required) }

          existing << required
        end
        existing.join(" ")
      end

      # A javascript: URL is still a pseudo-link when the scheme is spelled with
      # character references or separated by ASCII tab/newline; browsers decode
      # and normalize attribute-valued URLs before parsing the scheme.  The href
      # itself is only inspected, never rewritten.
      def pseudo_href?(href)
        value = decode_char_refs(href.to_s)
        value = value.gsub(/[\t\n\r]/, "").sub(/\A[\u0000-\u0020]+/, "")
        PSEUDO_SCHEME.match?(value)
      end

      # One decoding pass, like an HTML parser.  Numeric references may omit the
      # semicolon; the named ones that can appear in a scheme require it.  &amp;
      # is deliberately not decoded, so a double-encoded reference cannot
      # resolve into a scheme that browsers would not see.
      def decode_char_refs(value)
        decoded = value.gsub(NUMERIC_CHAR_REF) do |reference|
          match = Regexp.last_match
          codepoint = match[1] ? match[1].to_i : match[2].to_i(16)
          valid = codepoint.positive? && codepoint <= 0x10FFFF &&
                  !(0xD800..0xDFFF).cover?(codepoint)
          valid ? codepoint.chr(Encoding::UTF_8) : reference
        end
        decoded.gsub(NAMED_CHAR_REF) do |reference|
          NAMED_CHAR_REFS.fetch(reference[1..-2])
        end
      end

      def append_attributes(attributes, additions)
        return attributes if additions.empty?

        trailing = attributes[/\s*\z/] || ""
        core = attributes[0, attributes.length - trailing.length]
        # A trailing slash is a self-closing marker only when it is not the
        # last character of an unquoted value such as href=/team/.
        self_closing = core.match?(%r{(?:\A|\s|["'])/\z})
        core = core.sub(%r{\s?/\z}, "") if self_closing

        "#{core.rstrip} #{additions.join(' ')}#{self_closing ? ' /' : ''}#{trailing}"
      end

      # Tokenize attributes with byte offsets, respecting quoted values.  A
      # regex over the whole attribute string would match "rel=" inside
      # title="rel=..." and would lose the original spacing.
      def scan_attributes(attributes)
        tokens = []
        index = 0
        length = attributes.length

        while index < length
          index += 1 while index < length && attributes[index] =~ /\s/
          break if index >= length

          if attributes[index] == "/"
            index += 1
            next
          end

          name_start = index
          index += 1 while index < length && attributes[index] !~ /[\s=\/>]/
          break if index == name_start

          name = attributes[name_start...index]
          cursor = index
          cursor += 1 while cursor < length && attributes[cursor] =~ /\s/
          finish = index
          value = nil
          quote = nil
          value_start = nil
          value_finish = nil

          if cursor < length && attributes[cursor] == "="
            cursor += 1
            cursor += 1 while cursor < length && attributes[cursor] =~ /\s/

            if cursor < length && (attributes[cursor] == '"' || attributes[cursor] == "'")
              quote = attributes[cursor]
              value_start = cursor + 1
              cursor = value_start
              cursor += 1 while cursor < length && attributes[cursor] != quote
              value = attributes[value_start...cursor]
              value_finish = cursor
              cursor += 1 if cursor < length
            else
              value_start = cursor
              cursor += 1 while cursor < length && attributes[cursor] !~ /[\s>]/
              value = attributes[value_start...cursor]
              value_finish = cursor
            end
            finish = cursor
          end

          tokens << {
            name: name,
            value: value,
            quote: quote,
            start: name_start,
            finish: finish,
            value_start: value_start,
            value_finish: value_finish
          }
          index = finish
        end

        tokens
      end

      def inject_runtime(html)
        return html if html.match?(/\b#{RUNTIME_MARKER}\b/i)
        return html unless html.match?(/<html\b/i) || html.match?(/<body\b/i)

        inserted = false
        output = +""
        index = 0
        length = html.length

        while index < length
          opening = html.index("<", index)
          if opening.nil?
            output << html[index, length - index]
            break
          end

          output << html[index, opening - index] if opening > index

          # Skip raw-text regions so a literal "</body>" inside a script string
          # is not mistaken for the end of the document.
          protected_stop = protected_region_end(html, opening)
          if protected_stop
            output << html[opening, protected_stop - opening]
            index = protected_stop
            next
          end

          closing = /\G<\/body\s*>/i.match(html, opening)
          if !inserted && closing
            output << RUNTIME_SCRIPT << "\n" << closing[0]
            inserted = true
            index = closing.end(0)
            next
          end

          output << "<"
          index = opening + 1
        end

        inserted ? output : "#{html}\n#{RUNTIME_SCRIPT}"
      end
    end
  end
end

# Keep the module usable as a plain Ruby library in tests; register the hook
# only inside a real Jekyll process.
if defined?(Jekyll::Hooks)
  # :low runs after jekyll-link-attributes (:normal) so its external rel and
  # target can be merged rather than overwritten.  Posts are included because
  # some Jekyll versions trigger them as :posts rather than :documents.
  Jekyll::Hooks.register %i[pages documents posts], :post_render, priority: :low do |doc|
    doc.output = Jekyll::AllLinksInNewTabs.transform(doc.output) if doc.output.is_a?(String)
  end
end
