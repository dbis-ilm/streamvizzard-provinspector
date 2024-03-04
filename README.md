# ProvInspector

[![Made-with-Python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org) [![W3C-PROV](https://img.shields.io/badge/W3C-PROV-blue)](https://www.w3.org/TR/prov-overview/) [![License](https://img.shields.io/badge/license-Apache_2.0-green.svg)](https://opensource.org/licenses/Apache-2.0) [![Coverage](docs/assets/coverage-badge.svg)](README.md) [![Black](https://img.shields.io/badge/code%20style-black-black)](https://github.com/psf/black)

ProvInspector is a Python library that integrates support for provenance capturing, provenance graph generation, and provenance inspection into the StreamVizzard stream processing framework. The underlying [data model](docs/provenance_model.md) is compliant with the [W3C PROV](https://www.w3.org/TR/prov-overview/) specification.

## Installation

### Using pip

```bash
pip install git+https://dbgit.prakinf.tu-ilmenau.de/masc7357/provinspector
```

### Using Poetry

```bash
poetry install
```

The dependencies for development can be installed via Poetry's `--with` option:

```bash
poetry install --with dev
```

## License

This project is Apache 2.0 licensed. Copyright © 2024 by Marius Schlegel.
