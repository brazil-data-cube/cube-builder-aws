# BDC-Scripts-Spec

## Build Documentation

### Requirements

- [NodeJS 8+](https://nodejs.org/en/)
- [ReDoc](https://github.com/Redocly/redoc)

Execute the following command to install `node modules` dependencies:

```bash
npm install
```

After that, generate Oauth documentation:

```bash
npm run build
```

It will create folder `dist` with a bundled file `index.html`. You may serve this file with HTTP Server.
