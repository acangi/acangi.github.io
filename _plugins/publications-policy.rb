# Publications-page policy for the Jekyll build.
#
# /publications/ lists journal articles and preprints that have not yet been
# published in a journal. Everything else in papers.bib - conference talks and
# abstracts, conference papers, book chapters, books, technical reports,
# theses, datasets, software, repository-only records - stays in the
# bibliography but is not rendered on the page.
#
# The same rules are implemented for future imports in
# _scripts/openalex_to_yaml.py. Filtering happens here, before jekyll-scholar
# groups entries by year, because a layout-only filter would leave empty year
# headings behind.
#
# Preprint deduplication is the importer's job: it writes
# `publication_page = {false}` on preprints that have a journal version and on
# duplicate records. This tag honours that flag and never guesses at reworded
# titles. Preprints whose journal version can only be linked through an arXiv
# identifier that the bibliography does not carry are listed in
# _data/publication_policy.yml (hand-maintained, not generated); this tag and
# the importer both apply that list.
#
# Two defects in older data are guarded here as well, because a bibliography is
# not always regenerated together with the importer:
#   * a repository deposit typed @article (Zenodo/OSTI/figshare DOI or venue)
#     is not a journal article and stays off the page;
#   * an @article typed preprint is listed only while unpublished, and a
#     conclusive arxiv:journal_ref hides a preprint when the explicit flag is
#     absent. "Submitted to ..." or a bare journal name is not conclusive.
require 'jekyll'
require 'jekyll-scholar'

module Jekyll
  module PublicationsPolicy
    # Venues that host repository copies or datasets rather than journals.
    REPOSITORY_VENUE_MARKERS = %w[arxiv zenodo osti juser figshare rodare proquest].freeze
    # Venue words that mark conference proceedings or abstracts.
    CONFERENCE_VENUE_MARKERS = %w[conf symp workshop congress meeting].freeze
    # DOI prefixes of repositories and preprint servers.
    REPOSITORY_DOI_PREFIXES = %w[
      10.48550/ 10.5281/ 10.2172/ 10.34734/ 10.60893/
      10.14278/ 10.26226/ 10.11578/ 10.17605/ 10.6084/
      10.5061/ 10.17632/ 10.7910/ 10.1594/
    ].freeze

    # Phrases that arXiv authors put in arxiv:journal_ref to describe a
    # submission, not a publication. They must not hide an unpublished preprint.
    UNPUBLISHED_JOURNAL_REF_MARKERS = [
      'submitted', 'in preparation', 'in progress', 'under review',
      'in review', 'to be submitted', 'unpublished', 'not published',
      'draft', 'preprint'
    ].freeze

    module_function

    def flag_false(value)
      !value.nil? && %w[false 0 no].include?(value.to_s.strip.downcase)
    end

    def flag_present?(entry)
      !entry[:publication_page].nil?
    end

    def venue(entry)
      entry[:journal].to_s.strip
    end

    def doi(entry)
      entry[:doi].to_s.strip.downcase.sub(%r{\Ahttps?://(dx\.)?doi\.org/}, '')
    end

    def repository_doi?(entry)
      REPOSITORY_DOI_PREFIXES.any? { |prefix| doi(entry).start_with?(prefix) }
    end

    def repository_venue?(entry)
      name = venue(entry).downcase
      !name.empty? && REPOSITORY_VENUE_MARKERS.any? { |marker| name.include?(marker) }
    end

    # A repository deposit is not a journal article, whatever its BibTeX type
    # says. arXiv entries are preprints and are handled as such.
    def repository_copy?(entry)
      return false if preprint?(entry)

      repository_doi?(entry) || repository_venue?(entry)
    end

    # Only a publisher DOI proves that a preprint has a journal version; a
    # repository DOI (Zenodo, OSTI, figshare, ...) does not.
    def publisher_doi?(entry)
      !doi(entry).empty? && !repository_doi?(entry)
    end

    # arxiv:journal_ref is author-declared. Conclusive only when it is not a
    # submission status note and cites a volume, page, article number or year;
    # a bare journal name is too weak to hide a preprint.
    def conclusive_journal_ref?(entry)
      text = entry[:journal_ref].to_s.strip.downcase
      return false if text.empty?
      return false if UNPUBLISHED_JOURNAL_REF_MARKERS.any? { |marker| text.include?(marker) }

      text.match?(/\d/)
    end

    # A legacy entry typed @misc that carries a journal venue and a publisher
    # DOI is a journal article whose BibTeX type is stale (OpenAlex "other").
    def journal_article?(entry)
      name = venue(entry).downcase
      return false if name.empty?
      return false if repository_venue?(entry)
      return false if CONFERENCE_VENUE_MARKERS.any? { |marker| name.include?(marker) }
      return false if doi(entry).empty?

      !repository_doi?(entry)
    end

    def preprint?(entry)
      name = venue(entry).downcase
      name.include?('arxiv') || doi(entry).start_with?('10.48550/arxiv')
    end

    def arxiv_id_for(entry)
      text = [entry[:url], entry[:doi], entry[:arxiv], entry[:eprint]].map(&:to_s).join(' ')
      match = text.match(%r{arxiv\.org/(?:abs|pdf)/([^/?#\s]+)}i) ||
              text.match(%r{10\.48550/arxiv\.([0-9][0-9.]*)}i)
      return nil unless match

      match[1].sub(/\.pdf\z/i, '').sub(/v\d+\z/i, '')
    end

    def normalize_title(value)
      value.to_s.unicode_normalize(:nfkd)
           .encode('ASCII', invalid: :replace, undef: :replace)
           .downcase.gsub(/[^a-z0-9]+/, ' ').strip
    end

    # Curated overrides from _data/publication_policy.yml: preprints that are
    # published only under a different title/DOI than the importer can match.
    def override_supersedes?(entry, overrides)
      return false if overrides.nil? || overrides.empty?

      arxiv = arxiv_id_for(entry)
      title = normalize_title(entry[:title])
      overrides.any? do |override|
        next false unless override.is_a?(Hash)

        override_arxiv = override['arxiv'].to_s.strip
        override_title = normalize_title(override['title'])
        (!override_arxiv.empty? && arxiv == override_arxiv) ||
          (!override_title.empty? && title == override_title)
      end
    end

    # The explicit flag written by the importer wins when present. journal_ref
    # and a publisher DOI are fallbacks for legacy entries that predate the
    # flag, so an older bibliography is filtered without guessing.
    def preprint_hidden?(entry, overrides)
      return true if override_supersedes?(entry, overrides)
      return true if publisher_doi?(entry)
      return false if flag_present?(entry)

      conclusive_journal_ref?(entry)
    end

    def visible?(entry, overrides = nil)
      return false if flag_false(entry[:publication_page])

      case entry.type.to_s
      when 'article'
        # Older imports stored repository deposits and arXiv preprints as
        # @article. Neither is a journal article: a deposit is tracked but not
        # listed, and a preprint is listed only while it is unpublished.
        return false if repository_copy?(entry)

        !preprint?(entry) || !preprint_hidden?(entry, overrides)
      when 'misc'
        return false if preprint?(entry) && preprint_hidden?(entry, overrides)

        journal_article?(entry) || preprint?(entry)
      else
        false
      end
    end
  end

  # jekyll-scholar's bibliography tag with the policy applied to the entries
  # before they are grouped and sorted.
  class PublicationsBibliographyTag < Scholar::BibliographyTag
    def entries
      overrides = site.data.dig('publication_policy', 'superseded_preprints')
      all = super
      visible = all.select { |entry| PublicationsPolicy.visible?(entry, overrides) }
      log_policy(all, visible)
      visible
    end

    private

    # One audit line per build: a silently shrinking publications page is a
    # bug, an explained one is a policy decision.
    def log_policy(all, visible)
      return if @policy_logged

      @policy_logged = true
      hidden = all - visible
      by_type = hidden.group_by { |entry| entry.type.to_s }
                       .transform_values(&:size)
                       .sort
                       .map { |type, count| "#{type}=#{count}" }
                       .join(', ')
      Jekyll.logger.info(
        'Publications:',
        "#{visible.size} of #{all.size} bibliography entries on the page " \
        '(journal articles and unpublished preprints)' \
        "#{by_type.empty? ? '' : "; not shown: #{by_type}"}"
      )
    end
  end
end

Liquid::Template.register_tag('publications_bibliography', Jekyll::PublicationsBibliographyTag)
