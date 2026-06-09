# temporal-cpp-sdk documentation site

A [Docusaurus](https://docusaurus.io) site for the temporal-cpp-sdk docs, deployable to GitHub Pages.

## Local development

```bash
cd website
npm install
npm start          # dev server at http://localhost:3000
```

## Build

```bash
npm run build      # static site in ./build
npm run serve      # serve the built output locally
```

## Deploy to GitHub Pages

Pushing to `main` triggers [`.github/workflows/docs.yml`](../.github/workflows/docs.yml), which
builds the site and deploys it to GitHub Pages.

1. In the repo: **Settings → Pages → Source: GitHub Actions**.
2. The workflow passes your GitHub org/user via the `GH_USER` env, so the site URL
   (`https://<user>.github.io/temporal-cpp-sdk/`) is set automatically. To build locally with the right
   URL, run `GH_USER=<user> npm run build`, or edit `docusaurus.config.js`.

The documentation content lives in [`docs/`](./docs); the sidebar is [`sidebars.js`](./sidebars.js).
