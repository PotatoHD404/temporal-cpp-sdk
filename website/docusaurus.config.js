// @ts-check
// Docusaurus site config for the temporal-cpp documentation.
// Replace YOUR_GITHUB_USERNAME below (and the GitHub links) before deploying to
// GitHub Pages.
const {themes} = require('prism-react-renderer');

const ghUser = process.env.GH_USER || 'YOUR_GITHUB_USERNAME';
const repo = 'temporal-cpp';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'temporal-cpp',
  tagline: 'A native C++ SDK for Temporal',
  favicon: 'img/favicon.svg',

  url: `https://${ghUser}.github.io`,
  baseUrl: `/${repo}/`,

  organizationName: ghUser,
  projectName: repo,
  trailingSlash: false,

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

  i18n: {defaultLocale: 'en', locales: ['en']},

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          routeBasePath: '/', // serve docs at the site root
          editUrl: `https://github.com/${ghUser}/${repo}/tree/main/website/`,
        },
        blog: false,
        theme: {customCss: require.resolve('./src/css/custom.css')},
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {respectPrefersColorScheme: true},
      navbar: {
        title: 'temporal-cpp',
        items: [
          {type: 'docSidebar', sidebarId: 'docs', position: 'left', label: 'Docs'},
          {to: '/parity', label: 'Parity', position: 'left'},
          {href: `https://github.com/${ghUser}/${repo}`, label: 'GitHub', position: 'right'},
        ],
      },
      footer: {
        style: 'dark',
        copyright:
          'MIT licensed. An experimental, community project — not affiliated with or endorsed by Temporal Technologies, Inc.',
      },
      prism: {
        theme: themes.github,
        darkTheme: themes.dracula,
        additionalLanguages: ['cpp', 'bash', 'cmake', 'json'],
      },
    }),
};

module.exports = config;
