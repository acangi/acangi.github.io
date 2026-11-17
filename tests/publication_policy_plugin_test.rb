# Unit checks for _plugins/publications-policy.rb.
#
# Run with the repository's bundle (Ruby is not installed on every host):
#
#   bundle exec ruby tests/publication_policy_plugin_test.rb
#
# tests/test_plugin_publication_policy.py runs this file from unittest and
# fails when a check fails, so the Ruby filter and the Python importer are held
# to the same policy. The checks use a minimal entry object: the policy only
# needs BibTeX type and field lookup, so no full Jekyll site is built.
require_relative '../_plugins/publications-policy'

# Minimal stand-in for a jekyll-scholar entry: type plus BibTeX field lookup.
class FakeEntry
  attr_reader :type

  def initialize(type:, **fields)
    @type = type.to_s
    @fields = fields.each_with_object({}) do |(key, value), memo|
      memo[key.to_sym] = value
    end
  end

  def [](key)
    @fields[key.to_sym]
  end
end

$checks = []

def check(name, expected, actual)
  $checks << [name, expected, actual]
end

policy = Jekyll::PublicationsPolicy

overrides = [
  {
    'title' => 'Ultrahigh Resolution X-ray Thomson Scattering Measurements at the European XFEL',
    'arxiv' => '2403.02776',
    'published_as' => '10.1103/physrevb.109.l241112'
  }
]

# --- journal articles -------------------------------------------------------
check('journal article on a journal venue',
      true,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'Phys. Rev. B',
                                    doi: '10.1103/physrevb.109.l241112')))

check('article without a venue or DOI',
      true,
      policy.visible?(FakeEntry.new(type: :article)))

check('article published in a conference issue of a journal',
      true,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'Contrib. Plasma Phys.',
                                    doi: '10.1002/ctpp.202300110')))

# --- repository deposits typed @article -------------------------------------
check('article with a Zenodo venue and DOI',
      false,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'Zenodo (CERN European Organization for Nuclear Research)',
                                    doi: '10.5281/zenodo.12345')))

check('article with a figshare DOI',
      false,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'AIP Publishing',
                                    doi: '10.60893/figshare.jcp.c.8144699')))

check('article with an OSTI venue',
      false,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'OSTI OAI (U.S. Department of Energy Office of Scientific and Technical Information)',
                                    doi: '10.2172/2003393')))

# --- preprints typed @article (older imports) -------------------------------
check('legacy @article preprint without a journal version',
      true,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'arXiv',
                                    url: 'http://arxiv.org/abs/2401.00001v2')))

check('legacy @article preprint with a curated override',
      false,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'arXiv',
                                    url: 'http://arxiv.org/abs/2403.02776v3'),
                      overrides))

check('legacy @article preprint with a conclusive journal_ref',
      false,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'arXiv',
                                    journal_ref: 'Phys. Rev. B 109, L241112 (2024)')))

check('legacy @article preprint with a submitted journal_ref',
      true,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'arXiv',
                                    journal_ref: 'Submitted to Phys. Rev. B')))

check('legacy @article preprint with a bare journal_ref',
      true,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'arXiv',
                                    journal_ref: 'Phys. Rev. B')))

check('legacy @article preprint with a publisher DOI',
      false,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'arXiv',
                                    doi: '10.1103/physrevb.109.l241112')))

check('legacy @article preprint with a Zenodo DOI',
      true,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'arXiv',
                                    doi: '10.5281/zenodo.12345')))

check('@article preprint with an explicit flag keeps the journal_ref fallback out',
      true,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'arXiv',
                                    journal_ref: 'Phys. Rev. B 109, L241112 (2024)',
                                    publication_page: 'true')))

check('@article with an explicit false flag',
      false,
      policy.visible?(FakeEntry.new(type: :article,
                                    journal: 'Phys. Rev. B',
                                    doi: '10.1103/physrevb.100.000001',
                                    publication_page: 'false')))

# --- @misc entries ----------------------------------------------------------
check('@misc preprint flagged off the page',
      false,
      policy.visible?(FakeEntry.new(type: :misc,
                                    journal: 'arXiv',
                                    doi: '10.48550/arxiv.2401.00001',
                                    publication_page: 'false')))

check('@misc preprint flagged on the page',
      true,
      policy.visible?(FakeEntry.new(type: :misc,
                                    journal: 'arXiv',
                                    doi: '10.48550/arxiv.2401.00001',
                                    publication_page: 'true')))

check('@misc journal article rescued from a stale type',
      true,
      policy.visible?(FakeEntry.new(type: :misc,
                                    journal: 'Nat. Comput. Sci.',
                                    doi: '10.1038/s43588-024-00707-3')))

check('@misc repository record',
      false,
      policy.visible?(FakeEntry.new(type: :misc,
                                    journal: 'OSTI OAI (U.S. Department of Energy Office of Scientific and Technical Information)',
                                    doi: '10.2172/2003393')))

check('@misc preprint with a curated override',
      false,
      policy.visible?(FakeEntry.new(type: :misc,
                                    journal: 'arXiv',
                                    url: 'http://arxiv.org/abs/2403.02776v3'),
                      overrides))

check('@misc preprint with a conclusive journal_ref and no flag',
      false,
      policy.visible?(FakeEntry.new(type: :misc,
                                    journal: 'arXiv',
                                    journal_ref: 'Phys. Rev. B 109, L241112 (2024)')))

check('@misc preprint with an explicit flag keeps the journal_ref fallback out',
      true,
      policy.visible?(FakeEntry.new(type: :misc,
                                    journal: 'arXiv',
                                    journal_ref: 'Phys. Rev. B 109, L241112 (2024)',
                                    publication_page: 'true')))

check('@misc conference abstract',
      false,
      policy.visible?(FakeEntry.new(type: :misc,
                                    journal: 'APS Division of Plasma Physics Meeting Abstracts')))

# --- other entry types ------------------------------------------------------
check('proceedings paper',
      false,
      policy.visible?(FakeEntry.new(type: :inproceedings,
                                    booktitle: 'Proc. SciPy',
                                    doi: '10.25080/majora-212e5952-006')))

check('technical report',
      false,
      policy.visible?(FakeEntry.new(type: :techreport,
                                    institution: 'OSTI',
                                    doi: '10.2172/1677521')))

failures = $checks.reject { |_name, expected, actual| expected == actual }
$checks.each do |name, expected, actual|
  if expected == actual
    puts "ok - #{name}"
  else
    puts "not ok - #{name}: expected #{expected.inspect}, got #{actual.inspect}"
  end
end
puts "#{$checks.size} checks, #{failures.size} failures"
exit(failures.empty? ? 0 : 1)
