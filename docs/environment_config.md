# Configuração de Ambiente Verificada e Validada
**Data de Validação:** 12 de Dezembro de 2025
**Responsável:** Automatum Codex (AI Agent)

Este documento registra o estado real do hardware, software e as decisões arquiteturais validadas para o projeto **LeakHarvester**. Todas as especificações abaixo foram verificadas contra o ambiente físico do usuário e as versões mais recentes das bibliotecas disponíveis.

## 1. Hardware Local (Validado)

O sistema de desenvolvimento e produção (single-node) possui as seguintes especificações confirmadas:

| Componente | Especificação Detectada | Observações de Engenharia |
| :--- | :--- | :--- |
| **CPU** | **Intel Core i5-13600K** | 14 Cores (6P + 8E), 20 Threads. Suporte a AVX2 confirmado (essencial para Polars). Otimização de afinidade (P-cores) pode ser necessária. |
| **RAM** | **46 GiB Total** (~34 GiB Livres) | Capacidade superior ao padrão (32GB). Permite chunks de ingestão do Polars maiores (ex: 1-2GB) para maior throughput. |
| **Armazenamento (Root)** | **NVMe SSD (931.5 GB)** | Dispositivo `nvme0n1`. Alta IOPS. Espaço é o recurso mais escasso. Compressão ZSTD(9) é mandatória. |
| **Armazenamento (Sec)** | SSD SATA (447 GB) | Dispositivo `sda`. Pode ser usado como "Quarentena" ou backup de logs, aliviando o NVMe. |
| **GPU** | **NVIDIA GeForce RTX 5060** (8GB) | Driver 590.44.01, CUDA 13.1. Disponível para aceleração futura (ex: hashing massivo via cuDF), mas o pipeline principal foca em CPU/RAM para estabilidade de streaming. |

## 2. Stack de Software e Dependências (Verificado)

As versões abaixo representam o "Bleeding Edge" estável validado para Dezembro de 2025.

| Software/Lib | Versão Mínima | Status | Justificativa |
| :--- | :--- | :--- | :--- |
| **Python** | 3.12+ | ✅ Instalado | Performance superior em async e tipagem. |
| **uv** | Latest | ✅ Aprovado | Gerenciador de pacotes validado como "Production Ready" e ~10x mais rápido que pip. |
| **Polars** | `1.36.1` | ✅ Validado | Versão atual estável. A doc pedia `>=1.15.0`, o que é seguro. |
| **ClickHouse Connect** | `0.9.2` | ✅ Validado | Driver oficial. Suporte robusto a Arrow/Numpy. |
| **Typer** | `0.12.0+` | ✅ Validado | CLI Framework padrão da indústria moderna. |

## 3. Validação Arquitetural: ClickHouse Indices

A escolha entre **Token Bloom Filter (`tokenbf_v1`)** e **Inverted Index** foi reavaliada com base em benchmarks recentes:

*   **Veredito:** **`tokenbf_v1` Mantido.**
*   **Evidência:** Embora Índices Invertidos permitam buscas mais rápidas e complexas, sua pegada em disco é significativamente maior (mapas exatos de termos). Para o requisito de "eficiência extrema de armazenamento" em um único NVMe de 1TB, o `tokenbf_v1` (que ocupa ~0.1% do tamanho dos dados) é a única escolha viável para maximizar a retenção de dados. A latência de busca levemente maior é um trade-off aceito.

## 4. Próximos Passos de Configuração

1.  **Inicialização do Projeto:** `uv init` e configuração do `pyproject.toml`.
2.  **Estrutura de Diretórios:** Criação da Arquitetura Hexagonal.
3.  **Setup do ClickHouse:** Docker Compose com limites de recursos ajustados para o i5-13600K (evitando OOM no host).

---
*Este arquivo deve ser atualizado sempre que houver mudanças de hardware ou upgrade de dependências críticas.*
