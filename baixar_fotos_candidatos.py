#!/usr/bin/env python3
"""
Baixa arquivos ZIP com fotos de candidatos do CDN do TSE.

URL: https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes{ano}/fotos/foto_cand{ano}_{uf}_div.zip
"""

from __future__ import annotations

import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DESTINO_BASE = BASE_DIR / "dados" / "fotos"

ANOS_ELEICAO = [2024]

UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
    "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
    "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

URL_TEMPLATE = (
    "https://cdn.tse.jus.br/estatistica/sead/eleicoes/"
    "eleicoes{ano}/fotos/foto_cand{ano}_{uf}_div.zip"
)


def url_foto(ano: int, uf: str) -> str:
    return URL_TEMPLATE.format(ano=ano, uf=uf)


def baixar(url: str, destino: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "dados-eleicoes-basicos/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        destino.write_bytes(resp.read())


def main() -> int:
    ok = 0
    pulados = 0
    erros: list[str] = []

    for ano in ANOS_ELEICAO:
        pasta_ano = DESTINO_BASE / str(ano)
        pasta_ano.mkdir(parents=True, exist_ok=True)

        print(f"\n=== Eleição {ano} → {pasta_ano} ===")

        for uf in UFS:
            nome_arquivo = f"foto_cand{ano}_{uf}_div.zip"
            destino = pasta_ano / nome_arquivo
            url = url_foto(ano, uf)

            if destino.exists() and destino.stat().st_size > 0:
                print(f"  [{uf}] já existe, pulando ({destino.stat().st_size:,} bytes)")
                pulados += 1
                continue

            print(f"  [{uf}] baixando {url} ...", end=" ", flush=True)
            try:
                baixar(url, destino)
                print(f"ok ({destino.stat().st_size:,} bytes)")
                ok += 1
            except urllib.error.HTTPError as exc:
                destino.unlink(missing_ok=True)
                msg = f"HTTP {exc.code}"
                print(msg)
                erros.append(f"{ano}/{uf}: {msg}")
            except Exception as exc:
                destino.unlink(missing_ok=True)
                print(f"erro: {exc}")
                erros.append(f"{ano}/{uf}: {exc}")

    print(f"\nResumo: {ok} baixados, {pulados} pulados, {len(erros)} erros")
    if erros:
        for item in erros:
            print(f"  - {item}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
