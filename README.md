<!-- PROJECT SHIELDS -->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]


<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/polymarket/agents">
    <img src="docs/images/cli.png" alt="Polymarket Agents CLI" width="466" height="262">
  </a>

  <h3 align="center">Polymarket Agents</h3>

  <p align="center">
    Trade autonomously on Polymarket using AI agents
    <br />
    <a href="https://github.com/polymarket/agents"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/polymarket/agents">View Demo</a>
    ·
    <a href="https://github.com/polymarket/agents/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    ·
    <a href="https://github.com/polymarket/agents/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>


<!-- CONTENT -->
# Polymarket Agents

Polymarket Agents is a developer framework and set of utilities for building AI agents for Polymarket.

This code is open source under the MIT License. **Note:** using agents to trade on Polymarket is subject to the platform's [Terms of Service](#terms-of-service).

## Features

- Integration with the Polymarket API
- AI agent utilities tailored for prediction markets
- Local and remote RAG (Retrieval-Augmented Generation) support
- Data sourcing from betting services, news providers, and web search
- Comprehensive LLM tooling for prompt engineering

## Getting Started

This repository targets **Python 3.9**.

1. **Clone the repository**

   ```bash
   git clone https://github.com/polymarket/agents.git
   cd agents
   ```

2. **Create a virtual environment**

   - macOS/Linux:

     ```bash
     python3.9 -m venv .venv
     source .venv/bin/activate
     ```

   - Windows (PowerShell):

     ```powershell
     py -3.9 -m venv .venv
     .\.venv\Scripts\Activate.ps1
     ```

3. **Upgrade pip and install dependencies**

   ```bash
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Set up environment variables**

   Create a `.env` file in the project root (you can copy the example):

   ```bash
   cp .env.example .env
   ```

   Then set the following variables:

   ```dotenv
   POLYGON_WALLET_PRIVATE_KEY=""
   OPENAI_API_KEY=""
   ```

5. **Fund your wallet with USDC.**

6. **Run the CLI**

   ```bash
   python scripts/python/cli.py
   ```

   Or try the trading script:

   ```bash
   python agents/application/trade.py
   ```

7. **PYTHONPATH (when running outside Docker)**

   - macOS/Linux:

     ```bash
     export PYTHONPATH="."
     ```

   - Windows (PowerShell):

     ```powershell
     $env:PYTHONPATH = "."
     ```

8. **Docker (optional)**

   Use the provided helper scripts:

   ```bash
   ./scripts/bash/build-docker.sh
   ./scripts/bash/run-docker-dev.sh
   ```

## Architecture

Polymarket Agents uses a modular architecture so community members can contribute to and extend individual components.

### APIs

Connectors standardize data sources and order types:

- **`Chroma.py`** — ChromaDB integration for vectorizing news sources and other API data. You can add alternative vector DB implementations.
- **`Gamma.py`** — `GammaMarketClient` for interacting with the Polymarket Gamma API to fetch/parse market & event metadata. Includes methods to retrieve current/tradable markets and details for specific markets/events.
- **`Polymarket.py`** — High-level client that interacts with the Polymarket API to retrieve market/event data and execute orders on the Polymarket DEX. Includes utility functions for building/signing orders and testing API interactions.
- **`Objects.py`** — Pydantic data models representing trades, markets, events, and related entities.

### Scripts

Utilities for local environment management, server setup for remote runs, and the CLI for end users.

`cli.py` is the main entry point for interacting with the Polymarket API, retrieving relevant news, querying local data, prompting LLMs, and executing trades on Polymarket.

**Command format:**

```bash
python scripts/python/cli.py <command> [--flag value] [--flag value]
```

**Example: get all markets** — retrieve and display a list of markets from Polymarket, sorted by volume:

```bash
python scripts/python/cli.py get-all-markets --limit <LIMIT> --sort-by <SORT_BY>
```

- `--limit` — number of markets to retrieve (default: `5`)
- `--sort-by` — sorting criterion (default: `volume`)

## Contributing

Contributions are welcome!

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run linters/tests (and pre-commit hooks if enabled)
5. Open a pull request

Initialize pre-commit hooks:

```bash
pre-commit install
```

## Related Repositories

- [py-clob-client](https://github.com/Polymarket/py-clob-client) — Python client for the Polymarket CLOB
- [python-order-utils](https://github.com/Polymarket/python-order-utils) — Python utilities to generate/sign orders for the Polymarket CLOB
- [Polymarket CLOB client](https://github.com/Polymarket/clob-client) — TypeScript client for the Polymarket CLOB
- [LangChain](https://github.com/langchain-ai/langchain) — Utilities for building context-aware reasoning applications
- [Chroma](https://docs.trychroma.com/getting-started) — Open-source vector database

## Further Reading (Prediction Markets)

- Mikey 0x — *Prediction Markets: Bottlenecks and the Next Major Unlocks*: https://mirror.xyz/1kx.eth/jnQhA56Kx9p3RODKiGzqzHGGEODpbskivUUNdd7hwh0
- Vitalik Buterin — *The promise and challenges of crypto + AI applications*: https://vitalik.eth.limo/general/2024/01/30/cryptoai.html
- Schoemaker & Tetlock — *Superforecasting: How to Upgrade Your Company's Judgment*: https://hbr.org/2016/05/superforecasting-how-to-upgrade-your-companys-judgment

## License

This project is licensed under the MIT License. See [LICENSE](https://github.com/Polymarket/agents/blob/main/LICENSE.md) for details.

## Contact

Questions or inquiries: **liam@polymarket.com** • https://www.greenestreet.xyz

Enjoy using the CLI! If you encounter any issues, feel free to open one in the repository.

## Terms of Service

[Terms of Service](https://polymarket.com/tos) prohibit U.S. persons and persons from certain other jurisdictions from trading on Polymarket (via UI & API, including agents developed by persons in restricted jurisdictions), although data and information are viewable globally.


<!-- LINKS -->
[contributors-shield]: https://img.shields.io/github/contributors/polymarket/agents?style=for-the-badge
[contributors-url]: https://github.com/polymarket/agents/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/polymarket/agents?style=for-the-badge
[forks-url]: https://github.com/polymarket/agents/network/members
[stars-shield]: https://img.shields.io/github/stars/polymarket/agents?style=for-the-badge
[stars-url]: https://github.com/polymarket/agents/stargazers
[issues-shield]: https://img.shields.io/github/issues/polymarket/agents?style=for-the-badge
[issues-url]: https://github.com/polymarket/agents/issues
[license-shield]: https://img.shields.io/github/license/polymarket/agents?style=for-the-badge
[license-url]: https://github.com/polymarket/agents/blob/master/LICENSE.md
