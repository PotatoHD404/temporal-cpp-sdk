// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'intro',
    'getting-started',
    'installation',
    'tutorial',
    'concepts',
    {
      type: 'category',
      label: 'Writing workflows',
      collapsed: false,
      items: [
        'workflows/overview',
        'workflows/messaging',
        'workflows/composition',
      ],
    },
    'patterns',
    'client-and-worker',
    'data-conversion',
    'error-handling',
    'advanced',
    'production',
    'architecture',
    'testing',
    'parity',
  ],
};

module.exports = sidebars;
