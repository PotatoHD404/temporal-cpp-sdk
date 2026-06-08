// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'intro',
    'getting-started',
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
    'client-and-worker',
    'data-conversion',
    'architecture',
    'testing',
    'parity',
  ],
};

module.exports = sidebars;
