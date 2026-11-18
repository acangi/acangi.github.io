# frozen_string_literal: true

require "json"
require "ipaddr"
require "uri"

# Site-wide link policy for rendered pages and documents.
#
# Destinations are classified by where they actually lead, not by how the href
# was written:
#
# * Internal destinations navigate in the same browsing context.  This covers
#   relative paths, fragments, query strings and absolute URLs on the
#   configured site origin (host plus configured/default port).  They never
#   carry target="_blank"; an existing one left behind by an earlier
#   transformation is removed together with noopener/noreferrer.
# * External HTTP(S) destinations open in a new tab with
#
#       target="_blank" rel="noopener noreferrer"
#
#   Existing rel tokens are preserved and rel="opener" is removed.
# * Non-navigation destinations keep the browser default.  javascript:,
#   mailto:, tel:, sms:, data:, blob: and links carrying `download` are left
#   byte-for-byte unchanged, so mailto: is not forced into a new tab.
#
# The host comparison parses authority syntax instead of searching the string,
# so "https://acangi.github.io.evil.example/", "https://evil.example/?next=
# https://acangi.github.io/", credentials and non-default ports are external
# even though they contain the canonical host as text.  Special-scheme parsing
# mirrors WHATWG: "\" counts as "/" before the authority, so
# "https://evil.example\@acangi.github.io/" is external and
# "https://acangi.github.io\@evil.example/" is internal.  The runtime delegates
# the same decision to the browser's own URL parser.
#
# Server-rendered markup is rewritten here, after the layout has been rendered
# and after jekyll-link-attributes has handled external links.  A small runtime
# is injected into each HTML document as well, because some anchors are created
# by client-side JavaScript (Leaflet attribution, publication badges, ...) and
# never pass through Jekyll's renderer.  The runtime receives the configured
# site origin and also treats the page's actual window origin (scheme, host and
# port) as internal, so a local preview such as localhost:4000 behaves like the
# production site without ever trusting localhost from the production origin.
# The runtime writes target/rel only when their rendered values differ, because
# its MutationObserver watches those attributes and identical writes would feed
# back into the observer forever.
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
    DEFAULT_SITE_URL = "https://acangi.github.io"
    DEFAULT_SITE_HOST = URI.parse(DEFAULT_SITE_URL).host
    SECURITY_TOKENS = %w[noopener noreferrer].freeze
    RUNTIME_MARKER = "data-all-links-new-tab"
    LINK_TAGS = %w[a area].freeze
    HTTP_SCHEMES = %w[http https].freeze
    # Elements whose content is text, not markup, for HTML parsers.
    RAW_TEXT_ELEMENTS = %w[script style textarea title xmp].freeze
    SCHEME_PATTERN = /\A([A-Za-z][A-Za-z0-9+.\-]*):/
    # WHATWG special URLs treat "\" exactly like "/" when locating the
    # authority, so a host can be hidden after a backslash and a canonical
    # host can be hidden before one.
    AUTHORITY_TERMINATOR = %r{[/\\?#]}
    HOSTNAME_PATTERN = /\A(?=.{1,253}\z)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\z/i
    # Character references used only to recognise an encoded javascript:
    # scheme.  Browsers resolve these in attribute values before parsing the
    # URL, so "&#106;avascript&#58;" is a javascript: URL too.
    NUMERIC_CHAR_REF = /&#(?:(\d{1,7})|[xX](\h{1,6}));?/
    NAMED_CHAR_REF = /&(colon|Tab|NewLine);/
    NAMED_CHAR_REFS = { "colon" => ":", "Tab" => "\t", "NewLine" => "\n" }.freeze

    RUNTIME_TEMPLATE = <<~'JS'
      (function () {
        "use strict";
        var SITE_HOST = __SITE_HOST__;
        var SITE_PORT = __SITE_PORT__;
        var SITE_SCHEME = __SITE_SCHEME__;
        var TOKENS = ["noopener", "noreferrer"];
        function baseUrl() {
          try {
            if (typeof document !== "undefined" && document.baseURI) {
              return document.baseURI;
            }
            if (typeof window !== "undefined" && window.location &&
                window.location.href) {
              return window.location.href;
            }
          } catch (error) {
            /* non-browser environment */
          }
          return null;
        }
        function normalizeHost(host) {
          return String(host == null ? "" : host)
            .toLowerCase()
            .replace(/^\[|\]$/g, "")
            .replace(/\.$/, "");
        }
        function formatHost(host) {
          return host.indexOf(":") >= 0 ? "[" + host + "]" : host;
        }
        function originKey(scheme, host, port) {
          try {
            var text = scheme + "://" + formatHost(host) +
              (port ? ":" + String(port) : "");
            return new URL(text).origin;
          } catch (error) {
            return null;
          }
        }
        function addOrigin(origins, scheme, host, port) {
          var key = originKey(scheme, host, port);
          if (key && origins.indexOf(key) < 0) origins.push(key);
        }
        // The configured origin is internal, including the default ports on
        // both HTTP schemes exactly as the static transform classifies them.
        function configuredOrigins() {
          var host = normalizeHost(SITE_HOST);
          var scheme = String(SITE_SCHEME || "https").toLowerCase();
          var sibling = scheme === "http" ? "https" : "http";
          var schemes = [scheme, sibling];
          var ports = SITE_PORT ? [String(SITE_PORT)] : ["", "80", "443"];
          var origins = [];
          var i;
          var j;
          for (i = 0; i < schemes.length; i += 1) {
            for (j = 0; j < ports.length; j += 1) {
              addOrigin(origins, schemes[i], host, ports[j]);
            }
          }
          return origins;
        }
        var CONFIGURED_ORIGINS = configuredOrigins();
        // A preview page is served from its own origin (localhost:4000,
        // 127.0.0.1:8000, ...), which is internal too.  The match is scheme,
        // host and port, so localhost next to a 127.0.0.1 page remains
        // external.
        function currentOrigins() {
          try {
            var location = window.location;
            if (!location || !location.hostname) return [];
            var host = normalizeHost(location.hostname);
            var scheme = String(location.protocol || "https:")
              .replace(/:$/, "")
              .toLowerCase();
            var port = location.port
              ? String(location.port)
              : (scheme === "http" ? "80" : "443");
            var sibling = scheme === "http" ? "https" : "http";
            var origins = [];
            addOrigin(origins, scheme, host, port);
            addOrigin(origins, sibling, host, port);
            return origins;
          } catch (error) {
            /* no location in non-browser environments */
          }
          return [];
        }
        function isKnownOrigin(origin) {
          var origins = CONFIGURED_ORIGINS.concat(currentOrigins());
          for (var i = 0; i < origins.length; i += 1) {
            if (origins[i] === origin) return true;
          }
          return false;
        }
        function originOf(url) {
          return originKey(
            url.protocol.replace(/:$/, ""),
            normalizeHost(url.hostname),
            url.port
          );
        }
        function policy(value, element) {
          if (element && typeof element.hasAttribute === "function" &&
              element.hasAttribute("download")) return "ignore";
          var text = value == null ? "" : String(value);
          // An empty href stays untouched, matching the static transform.
          if (!text.replace(/[\u0000-\u0020]/g, "")) return "ignore";
          // Delegate to the browser's own URL parser.  It applies WHATWG
          // special-URL rules (backslash as slash, same-scheme relative
          // references, tab/newline stripping, IDNA and IPv6 normalization),
          // so the runtime cannot disagree with where the link actually goes.
          var base = baseUrl();
          var parsed;
          try {
            parsed = base ? new URL(text, base) : new URL(text);
          } catch (error) {
            // A URL the browser cannot resolve is external: the fail-safe
            // direction is a new tab with noopener and noreferrer.
            return "external";
          }
          if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
            return "ignore";
          }
          return isKnownOrigin(originOf(parsed)) ? "internal" : "external";
        }
        function dropAttribute(element, name) {
          if (typeof element.removeAttribute === "function") {
            element.removeAttribute(name);
          } else {
            element.setAttribute(name, "");
          }
        }
        function openInNewTab(element) {
          // MutationObserver watches target/rel.  Writing an identical value
          // still queues a mutation record in browsers, so only write when
          // the rendered string actually differs; otherwise secure() would
          // re-trigger itself forever.
          if (element.getAttribute("target") !== "_blank") {
            element.setAttribute("target", "_blank");
          }
          var rel = element.getAttribute("rel");
          var tokens = (rel || "").split(/\s+/);
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
          var newRel = kept.join(" ");
          if (rel !== newRel) element.setAttribute("rel", newRel);
        }
        function stayInSameTab(element) {
          var target = element.getAttribute("target");
          if (target !== null && String(target).toLowerCase() === "_blank") {
            dropAttribute(element, "target");
          }
          var rel = element.getAttribute("rel");
          if (rel === null || rel === "") return;
          var tokens = String(rel).split(/\s+/);
          var kept = [];
          var i;
          for (i = 0; i < tokens.length; i += 1) {
            if (!tokens[i]) continue;
            var token = tokens[i].toLowerCase();
            if (token !== "noopener" && token !== "noreferrer") kept.push(tokens[i]);
          }
          if (kept.join(" ") === rel) return;
          if (kept.length === 0) {
            dropAttribute(element, "rel");
          } else {
            element.setAttribute("rel", kept.join(" "));
          }
        }
        function secure(element) {
          if (!element) return;
          var tag = (element.tagName || "").toUpperCase();
          if (tag !== "A" && tag !== "AREA") return;
          var href = element.getAttribute("href");
          if (href === null) return;
          var kind = policy(href, element);
          if (kind === "external") openInNewTab(element);
          else if (kind === "internal") stayInSameTab(element);
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
            attributeFilter: ["href", "target", "rel", "download"]
          });
        }
      })();
    JS

    class << self
      def transform(document, site_url: nil)
        return document unless document.is_a?(String) && !document.empty?

        origin = site_origin(site_url)
        output = rewrite_tags(document, origin)
        inject_runtime(output, origin)
      end

      private

      # Host, scheme and port of the configured canonical site URL.  The
      # Jekyll hook passes the site's own `url`; standalone use falls back to
      # JEKYLL_SITE_URL and then to this site's production URL.  A malformed
      # value never becomes a host (or scheme): it falls back to the known
      # production origin instead of making every link external or internal.
      def site_origin(site_url)
        candidate = site_url.to_s.strip
        candidate = ENV["JEKYLL_SITE_URL"].to_s.strip if candidate.empty?
        candidate = DEFAULT_SITE_URL if candidate.empty?

        if candidate.match?(%r{\A[A-Za-z][A-Za-z0-9+.\-]*://})
          # Explicit absolute URL.
        elsif candidate.match?(%r{\A(?:\[[^\]]+\]|[A-Za-z0-9.\-]+):\d+(?:[/?#]|\z)})
          # "host:port" without a scheme.
          candidate = "//#{candidate}"
        elsif candidate.match?(%r{\A[A-Za-z][A-Za-z0-9+.\-]*:})
          # A scheme without "//" is a relative reference in a browser, not an
          # origin that a site URL could designate.
          return default_origin
        else
          candidate = "//#{candidate}"
        end

        origin_from_parts(parse_url(candidate)) || default_origin
      rescue StandardError
        default_origin
      end

      def default_origin
        { host: DEFAULT_SITE_HOST, scheme: "https", port: nil }
      end

      def origin_from_parts(parts)
        return nil unless parts[:host] && !parts[:relative] && !parts[:malformed]

        scheme = parts[:scheme] || "https"
        return nil unless HTTP_SCHEMES.include?(scheme)

        host = parts[:host].to_s
        return nil unless valid_host?(host)

        port = parts[:port]
        if port
          return nil unless port.match?(/\A\d{1,5}\z/)

          port = port.to_i
          return nil unless port.between?(1, 65_535)

          port = port.to_s
        end

        { host: host, scheme: scheme, port: port }
      end

      def valid_host?(host)
        return false if host.nil? || host.empty?

        if host.include?(":")
          begin
            return IPAddr.new(host).ipv6?
          rescue StandardError
            return false
          end
        end

        HOSTNAME_PATTERN.match?(host)
      end

      # Walk the document as a tokenizer: text runs are copied, protected
      # regions are copied verbatim, and only a start tag whose own name is a
      # link is rewritten.  A "<a" inside another tag's quoted attribute is
      # therefore never seen as markup.
      def rewrite_tags(document, origin)
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
            output << "<#{name}#{rewrite_attributes(attributes, origin)}>"
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

      def rewrite_attributes(attributes, origin)
        tokens = scan_attributes(attributes)
        href_token = tokens.find { |token| token[:name].casecmp?("href") }
        return attributes if href_token.nil?

        href = href_token[:value].to_s.strip
        return attributes if href.empty?
        # download belongs to the browser's download manager, not to a tab.
        return attributes if tokens.any? { |token| token[:name].casecmp?("download") }

        case destination_policy(href, origin)
        when :external
          rewrite_external(attributes, tokens)
        when :internal
          rewrite_internal(attributes, tokens)
        else
          attributes
        end
      end

      def rewrite_external(attributes, tokens)
        target_token = tokens.find { |token| token[:name].casecmp?("target") }
        rel_token = tokens.find { |token| token[:name].casecmp?("rel") }
        target = target_token && target_token[:value].to_s.strip
        blank = target && target.casecmp?("_blank")

        edits = []
        additions = []

        unless blank
          if target_token.nil?
            additions << 'target="_blank"'
          elsif target_token[:value].nil?
            edits << [target_token[:start], target_token[:finish], 'target="_blank"']
          else
            edits << [target_token[:value_start], target_token[:value_finish], "_blank"]
          end
        end

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

        # Apply from right to left so earlier byte offsets stay valid.
        edits.sort_by { |start, _finish, _replacement| -start }.each do |start, finish, replacement|
          attributes[start...finish] = replacement
        end

        append_attributes(attributes, additions)
      end

      # Same-tab links must not keep a blank target or the security tokens that
      # only make sense for a new tab.  Other rel tokens such as nofollow are
      # untouched, and an empty rel is removed rather than left as rel="".
      def rewrite_internal(attributes, tokens)
        target_token = tokens.find { |token| token[:name].casecmp?("target") }
        rel_token = tokens.find { |token| token[:name].casecmp?("rel") }
        edits = []

        target = target_token && target_token[:value].to_s.strip
        if target && target.casecmp?("_blank")
          edits << removal_range(attributes, target_token)
        end

        if rel_token && !rel_token[:value].nil?
          existing = rel_token[:value].split(/\s+/).reject(&:empty?)
          kept = existing.reject do |token|
            SECURITY_TOKENS.any? { |required| token.casecmp?(required) }
          end
          if kept.length != existing.length
            if kept.empty?
              edits << removal_range(attributes, rel_token)
            else
              edits << [rel_token[:value_start], rel_token[:value_finish], kept.join(" ")]
            end
          end
        end

        edits.sort_by { |start, _finish, _replacement| -start }.each do |start, finish, replacement|
          attributes[start...finish] = replacement
        end

        attributes
      end

      def removal_range(attributes, token)
        start = token[:start]
        start -= 1 while start.positive? && attributes[start - 1] =~ /\s/
        [start, token[:finish], ""]
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

      # Classify an href by the destination a browser would resolve.  The URL
      # is inspected only, never rewritten: character references and ASCII
      # tab/newline are normalized first, exactly as URL parsing does.
      def destination_policy(href, origin)
        parts = parse_url(href, base_scheme: origin[:scheme])
        return :ignore if parts.empty?
        return :internal if parts[:relative]
        return :ignore if parts[:scheme] && !HTTP_SCHEMES.include?(parts[:scheme])
        return :external if parts[:malformed] || parts[:host].nil?

        internal_destination?(parts, origin) ? :internal : :external
      end

      def internal_destination?(parts, origin)
        host = parts[:host]
        return false if host.nil? || host.empty?
        # The static transform only knows the configured site origin.  A
        # localhost link in a production build is a different host, so it is
        # external; the runtime can still recognise the actual preview origin.
        return false unless host == origin[:host]

        if origin[:port]
          # A configured non-default port is part of the origin's identity.
          href_port = parts[:port] || default_port(parts[:scheme] || origin[:scheme])
          return href_port == origin[:port]
        end

        port = parts[:port]
        return true if port.nil?

        %w[80 443].include?(port)
      end

      def default_port(scheme)
        scheme == "http" ? "80" : "443"
      end

      # Parse only enough of the URL to find the browser-visible host: scheme,
      # authority, userinfo, IPv6 literals and port.  Returns {} when the value
      # is empty, { relative: true } for a same-document/path reference, a
      # scheme-only hash for non-HTTP schemes, and { malformed: true } when an
      # HTTP URL has no usable authority.
      #
      # For special schemes this follows WHATWG: "\" is treated as "/" before
      # the authority, a scheme equal to the base scheme without "//" is a
      # relative reference, and any run of slashes/backslashes after the
      # scheme introduces the authority.  That is what makes the browser's
      # host differ from the text after the last "@" in URLs such as
      # "https://evil.example\@acangi.github.io/" (external) and
      # "https://acangi.github.io\@evil.example/" (internal).
      def parse_url(value, base_scheme: nil)
        text = normalize_url_text(value.to_s)
        return {} if text.empty?

        scheme = nil
        if (match = SCHEME_PATTERN.match(text))
          scheme = match[1].downcase
          return { scheme: scheme } unless HTTP_SCHEMES.include?(scheme)

          remainder = text[match.end(0)..].to_s
          # A special URL whose scheme equals the base scheme is resolved
          # relative to the page unless "//" (or "\\") starts an authority.
          return { relative: true } if base_scheme == scheme &&
                                       !special_authority_start?(remainder)

          rest = strip_authority_prefix(remainder)
        elsif special_authority_start?(text)
          rest = strip_authority_prefix(text)
        else
          return { relative: true }
        end

        authority = rest.split(AUTHORITY_TERMINATOR, 2).first.to_s
        return { scheme: scheme, malformed: true } if authority.empty?

        host, port = split_authority(authority)
        return { scheme: scheme, malformed: true } if host.nil? || host.empty?

        { scheme: scheme, host: host, port: port }
      end

      def normalize_url_text(value)
        text = decode_char_refs(value)
        text.gsub(/[\t\n\r]/, "").sub(/\A[\u0000-\u0020]+/, "").sub(/[\u0000-\u0020]+\z/, "")
      end

      # Two leading slash/backslash characters start an authority for a
      # special URL; a single leading one is an ordinary relative path.
      def special_authority_start?(text)
        text.length >= 2 && text[0].match?(%r{[/\\]}) && text[1].match?(%r{[/\\]})
      end

      # "special authority ignore slashes state": every leading slash and
      # backslash is consumed before the authority begins.
      def strip_authority_prefix(text)
        text.sub(%r{\A[/\\]+}, "")
      end

      def split_authority(authority)
        host_port = authority.rpartition("@").last
        port = nil

        if host_port.start_with?("[")
          close = host_port.index("]")
          return [nil, nil] if close.nil?

          host = host_port[1...close]
          after = host_port[(close + 1)..].to_s
          port = after[1..] if after.start_with?(":")
        else
          host = host_port
          if (colon = host_port.rindex(":"))
            candidate = host_port[(colon + 1)..].to_s
            if candidate.match?(/\A\d*\z/)
              host = host_port[0...colon]
              port = candidate
            end
          end
        end

        host = host.to_s.downcase.sub(/\.\z/, "")
        port = nil if port.nil? || port.empty?
        [host, port]
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

      def runtime_script(origin)
        runtime = RUNTIME_TEMPLATE
                  .sub("__SITE_HOST__") { JSON.generate(origin[:host]) }
                  .sub("__SITE_PORT__") { JSON.generate(origin[:port].to_s) }
                  .sub("__SITE_SCHEME__") { JSON.generate(origin[:scheme].to_s) }
        "<script #{RUNTIME_MARKER}>\n#{runtime}</script>"
      end

      def inject_runtime(html, origin)
        return html if html.match?(/\b#{RUNTIME_MARKER}\b/i)
        return html unless html.match?(/<html\b/i) || html.match?(/<body\b/i)

        script = runtime_script(origin)
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
            output << script << "\n" << closing[0]
            inserted = true
            index = closing.end(0)
            next
          end

          output << "<"
          index = opening + 1
        end

        inserted ? output : "#{html}\n#{script}"
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
    next unless doc.output.is_a?(String)

    site_url = doc.site.config["url"] if doc.respond_to?(:site) && doc.site
    doc.output = Jekyll::AllLinksInNewTabs.transform(doc.output, site_url: site_url)
  end
end
