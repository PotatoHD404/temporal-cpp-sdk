// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'intro',
    {
      type: 'category',
      label: 'Get started',
      collapsed: false,
      items: ['installation', 'getting-started', 'tutorial'],
    },
    'concepts',
    {
      type: 'category',
      label: 'Writing workflows',
      collapsed: false,
      items: [
        'workflows/overview',
        'workflows/messaging',
        'workflows/composition',
        'patterns',
      ],
    },
    {
      type: 'category',
      label: 'Failure handling',
      items: ['timeouts-and-retries', 'error-handling', 'cancellation'],
    },
    {
      type: 'category',
      label: 'Client, workers & scheduling',
      items: ['client-and-worker', 'schedules', 'data-conversion'],
    },
    {
      type: 'category',
      label: 'Operate',
      items: ['observability', 'production', 'testing', 'versioning'],
    },
    {
      type: 'category',
      label: 'Reference',
      items: ['advanced', 'architecture', 'parity'],
    },
  ],
};

module.exports = sidebars;
