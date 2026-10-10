"""Testes unitários do parser do corpo da mensagem (contrato produtor/consumidor).

Validam :func:`app.consumidores.payload_mensagem.extrair_payload`:

- corpo JSON completo do produtor → extrai os três campos;
- corpo JSON com cenário inválido → ``cenario=None`` (sem interromper o parsing);
- corpo não-JSON (ex.: ``"pedido-0001"``) → fallback: ``order_id=body``, demais ``None``;
- JSON que não é objeto (``"[]"``, ``"123"``) → fallback: ``order_id=body``.
"""

from __future__ import annotations

import json

from app.consumidores.payload_mensagem import PayloadMensagem, extrair_payload
from app.consumidores.processamento import CenarioFalha


def test_corpo_json_completo_extrai_os_tres_campos() -> None:
    corpo = json.dumps(
        {
            "order_id": "pedido-0001",
            "processing_seconds": 10,
            "cenario": "sucesso",
        }
    )

    payload = extrair_payload(corpo)

    assert payload == PayloadMensagem(
        order_id="pedido-0001",
        processing_seconds=10,
        cenario=CenarioFalha.SUCESSO,
    )


def test_corpo_json_com_cenario_desconhecido_resulta_em_cenario_none() -> None:
    corpo = json.dumps(
        {
            "order_id": "pedido-0002",
            "processing_seconds": 5,
            "cenario": "cenario_que_nao_existe",
        }
    )

    payload = extrair_payload(corpo)

    assert payload.order_id == "pedido-0002"
    assert payload.processing_seconds == 5
    assert payload.cenario is None


def test_corpo_json_com_processing_seconds_invalido_resulta_em_none() -> None:
    corpo = json.dumps(
        {
            "order_id": "pedido-0003",
            "processing_seconds": "nao-e-numero",
            "cenario": "excecao_proposital",
        }
    )

    payload = extrair_payload(corpo)

    assert payload.order_id == "pedido-0003"
    assert payload.processing_seconds is None
    assert payload.cenario is CenarioFalha.EXCECAO_PROPOSITAL


def test_corpo_nao_json_usa_fallback_order_id_igual_ao_corpo() -> None:
    payload = extrair_payload("pedido-0001")

    assert payload == PayloadMensagem(
        order_id="pedido-0001",
        processing_seconds=None,
        cenario=None,
    )


def test_json_que_nao_eh_objeto_lista_usa_fallback() -> None:
    # "[]" é JSON válido, mas não é um objeto: cai no fallback (corpo cru).
    payload = extrair_payload("[]")

    assert payload.order_id == "[]"
    assert payload.processing_seconds is None
    assert payload.cenario is None


def test_json_que_nao_eh_objeto_numero_usa_fallback() -> None:
    # "123" é JSON válido (número), mas não é um objeto: fallback.
    payload = extrair_payload("123")

    assert payload.order_id == "123"
    assert payload.processing_seconds is None
    assert payload.cenario is None


def test_objeto_json_sem_order_id_usa_fallback() -> None:
    # Objeto JSON sem a chave order_id: o contrato não se aplica; fallback.
    corpo = json.dumps({"processing_seconds": 10, "cenario": "sucesso"})

    payload = extrair_payload(corpo)

    assert payload.order_id == corpo
    assert payload.processing_seconds is None
    assert payload.cenario is None


def test_corpo_vazio_resulta_em_order_id_none() -> None:
    payload = extrair_payload("")

    assert payload.order_id is None
    assert payload.processing_seconds is None
    assert payload.cenario is None
