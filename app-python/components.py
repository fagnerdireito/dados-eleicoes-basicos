"""Formatadores e componentes visuais reaproveitados pelas abas."""
from __future__ import annotations

import streamlit as st


def fmt_int(n) -> str:
    if n is None:
        return "—"
    try:
        return f"{int(n):,}".replace(",", ".")
    except (TypeError, ValueError):
        return str(n)


def fmt_pct(n, casas: int = 2) -> str:
    if n is None:
        return "—"
    try:
        return f"{float(n):.{casas}f}%".replace(".", ",")
    except (TypeError, ValueError):
        return str(n)


def kpi(label: str, value: str, hint: str | None = None) -> None:
    with st.container(border=True):
        st.markdown(
            f"<div style='color:#5b6b80;font-size:0.85rem'>{label}</div>"
            f"<div style='color:#1f6feb;font-size:2.2rem;font-weight:700;line-height:1.1'>{value}</div>"
            + (f"<div style='color:#5b6b80;font-size:0.8rem'>{hint}</div>" if hint else ""),
            unsafe_allow_html=True,
        )


def section_title(title: str, subtitle: str | None = None) -> None:
    st.markdown(
        f"<div style='border-left:4px solid #1f6feb;padding-left:0.6rem;margin:0.4rem 0 1rem 0'>"
        f"<div style='font-size:1.6rem;font-weight:700;color:#0b2545'>{title}</div>"
        + (f"<div style='color:#5b6b80'>{subtitle}</div>" if subtitle else "")
        + "</div>",
        unsafe_allow_html=True,
    )


def bar_row(name: str, party: str, votos: int, pct: float, rank: int, color: str = "#1f6feb") -> None:
    pct_safe = max(0.0, min(100.0, float(pct or 0)))
    st.markdown(
        f"""
        <div style='display:flex;gap:0.6rem;align-items:center;padding:0.4rem 0;border-bottom:1px solid #eef1f5'>
          <div style='width:36px;color:#5b6b80;font-weight:700'>{rank}º</div>
          <div style='flex:1'>
            <div style='font-weight:600;color:#0b2545'>{name} <span style='color:#5b6b80;font-weight:400;font-size:0.85rem'>{party}</span></div>
            <div style='background:#eef1f5;border-radius:6px;height:6px;margin-top:4px;overflow:hidden'>
              <div style='background:{color};width:{pct_safe:.2f}%;height:100%'></div>
            </div>
          </div>
          <div style='width:120px;text-align:right'>
            <div style='font-weight:700;color:#0b2545'>{fmt_int(votos)}</div>
            <div style='color:#5b6b80;font-size:0.85rem'>{fmt_pct(pct)}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
