# Análise de Despesas de Operadoras de Plano de Saúde

**Autor**: Álison Caique  
**Data**: Fevereiro 2026  
**Propósito**: Teste técnico para vaga de estágio

---

## Resumo rápido

Este projeto foi desenvolvido no contexto de um desafio técnico para uma empresa do setor de healthtech, utilizando exclusivamente dados públicos da ANS (Agência Nacional de Saúde Suplementar).
Foram implementadas a **INTEGRAÇÃO COM API PÚBLICA** e a **TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS**.

O sistema foi projetado para lidar com **dados reais e inconsistentes**, priorizando:
- qualidade de dados,
- rastreabilidade,
- decisões técnicas justificadas,
- clareza de código e documentação.

> **Não foram implementados nesta entrega:**  
> Banco de dados e API / Interface Web. Mais detalhes na seção Notas, ao final deste arquivo.

**Leitura recomendada para avaliadores:**
- O que o sistema faz  
- Como executar  
- Estrutura do projeto  
- Decisões técnicas importantes
- Limitações conhecidas  

---

## O que o sistema faz

Este projeto baixa, processa e consolida dados de demonstrações contábeis de operadoras de planos de saúde da ANS (Agência Nacional de Saúde Suplementar). 

O sistema:
1. Baixa automaticamente os últimos 3 trimestres de dados da ANS
2. Valida CNPJs e filtra dados inválidos
3. Correlaciona despesas com cadastro de operadoras
4. Gera relatórios consolidados com estatísticas
5. Identifica e reporta inconsistências nos dados

## Como executar

```bash
# Instalar dependências
pip install pandas requests beautifulsoup4 openpyxl

# Executar
python src/main.py
```

## Estrutura do projeto

```
contabil-cli/
├── src/
│   ├── main.py                    # Fluxo principal do programa
│   └── processamento_dados.py     # Funções auxiliares
├── dados_consolidados/            # Arquivos gerados pelo sistema
└── README.md
```

## Arquivos gerados

| Arquivo | Descrição |
|---------|-----------|
| `dados_processados_com_operadoras.csv` | **Arquivo principal** com dados consolidados + info de operadoras |
| `demonstracoes_contabeis_consolidadas.csv` | Dados consolidados sem join com operadoras |
| `*_cnpjs_invalidos.csv` | CNPJs que falharam na validação |
| `*_cnpjs_duplicados.csv` | CNPJs com razões sociais diferentes |
| `*_operadoras_inconsistencias.csv` | Operadoras com dados conflitantes no cadastro |
| `*_desvio_padrao.csv` | Desvio padrão das despesas por operadora |

## Colunas do arquivo principal

- `CNPJ`: CNPJ da operadora (validado)
- `RazaoSocial`: Nome da operadora (único por CNPJ)
- `Trimestre`: Últimos 3 Trimestres
- `Ano`: Ano de referência (mais recente)
- `ValorDespesas`: Total de despesas no trimestre
- `MediaTrimestral`: Média das despesas individuais do trimestre
- `RegistroANS`: Registro da operadora na ANS
- `Modalidade`: Tipo de plano (Medicina de Grupo, Cooperativa, etc)
- `UF`: Estado da operadora

## Decisões técnicas importantes

### 1. Por que processar um trimestre por vez?

**Problema**: Baixar e processar 3 trimestres de uma vez consome muita memória.

**Solução**: Baixo um arquivo ZIP por vez, processo, e descarto antes de baixar o próximo.

**Trade-off**: 
- ✅ Reduz uso de memória (importante para máquinas com pouca RAM)
- ❌ Mais lento que processar tudo em paralelo
- **Decisão**: Escolhi memória baixa, pois é mais importante para estabilidade

### 2. Por que validar CNPJs?

**Problema**: Encontrei CNPJs inválidos nos dados da ANS (ex: "00000000000000", dígitos verificadores errados).

**Solução**: Implementei validação com algoritmo de módulo 11 (algoritmo oficial do nosso país).

**Resultado**: ~X% dos CNPJs eram inválidos e foram separados para análise.

### 3. Por que usar busca dinâmica de colunas?

**Problema**: Cada trimestre pode ter nomes de colunas diferentes:
- Alguns usam `REG_ANS`
- Outros usam `REGISTRO_OPERADORA`
- Outros usam `CD_REGISTRO_ANS`
- Não realizei uma busca extensiva, mas pode haver outros nomes

**Solução**: O código tenta múltiplos nomes possíveis até encontrar um que existe.

**Trade-off**:
- ✅ Código funciona mesmo quando ANS muda nomes de colunas
- ❌ Mais complexo que usar nome fixo
- **Decisão**: Vale a pena, pois encontrei variações reais nos dados (prints de debug também ajudam a identificar novos nomes)

### 4. Por que salvar arquivo temporário de operadoras?

**Problema**: A função `tempfile.NamedTemporaryFile(delete=False)` cria arquivos que não são deletados automaticamente.

**Solução**: Deletei manualmente com `os.unlink()` logo após carregar o DataFrame.

**Por que não usar `delete=True`?** Porque o arquivo seria deletado antes de conseguir ler. Com `delete=False`, controlo quando deletar.

### 5. Como funciona a agregação de dados?

**Problema**: Uma operadora pode ter múltiplas linhas de despesas no mesmo trimestre.

**Solução**: Agrupei por (CNPJ + Trimestre + Ano) e somei as despesas.

**Exemplo real**:
```
Antes:
CNPJ | Trimestre | Despesa
1234 | 1         | 100      <- Despesa tipo A
1234 | 1         | 200      <- Despesa tipo B

Depois:
CNPJ | Trimestre | ValorDespesas | MediaTrimestral
1234 | 1         | 300           | 150
```

- `ValorDespesas`: Soma (100+200 = 300)
- `MediaTrimestral`: Média (300/2 = 150)

## Limitações conhecidas

1. **Sem tratamento de quedas de conexão**: Se a internet cair durante download, o programa falha.
2. **Sem cache de downloads**: Baixa tudo de novo a cada execução.
3. **Sem paralelização**: Processa um trimestre por vez (poderia processar em paralelo).
4. **Encoding fixo**: Assume `latin1` para CSVs (pode falhar com outros encodings).
5. **Sem validação de valores**: Não verifica se despesas são positivas ou realistas.

---

## ANEXO A: Lições Aprendidas

### Resumo dos problemas encontrados e soluções

#### Problema 1: Arquivos temporários não eram deletados

**Erro**: Sistema deixava arquivos `.csv` no diretório temporário do Windows.

**Causa**: `tempfile.NamedTemporaryFile(delete=False)` não deleta automaticamente.

**Solução**: Adicionei `os.unlink()` após usar o arquivo.

#### Problema 2: TemporaryDirectory deletava arquivos antes de processar

**Erro**: Tentei processar arquivos fora do bloco `with`, mas eles já tinham sido deletados.

**Causa**: `TemporaryDirectory` deleta tudo quando o bloco `with` termina.

**Solução**: Movi o processamento **para dentro** do bloco `with`.

#### Problema 3: CNPJs duplicados com dados diferentes

**Erro**: Mesmo CNPJ aparecia com múltiplas razões sociais.

**Causa**: Erros de cadastro ou mudanças de nome não atualizadas.

**Solução**: Salvei em arquivo separado para revisão manual, mas mantive todos os dados (rastreabilidade).

#### Problema 4: Join multiplicava linhas

**Erro**: Join com operadoras criava linhas duplicadas.

**Causa**: Havia CNPJs duplicados no cadastro de operadoras.

**Solução**: Usei `drop_duplicates(subset=['CNPJ'], keep='first')` antes do join.

### Tecnologias usadas

- **Python 3.11**
- **Pandas**: Manipulação de dados
- **Requests**: Download de arquivos
- **BeautifulSoup**: Parsing de HTML
- **zipfile**: Extração de arquivos
- **tempfile**: Gerenciamento de arquivos temporários

### Lições destacadas

1. **Sempre validar dados externos**: CNPJs inválidos e duplicatas são comuns em bases públicas
2. **Busca dinâmica de colunas**: Hardcoded names quebram quando a fonte muda
3. **Agregação é essencial**: Dados brutos raramente estão no formato ideal para análise
4. **Performance importa**: `iterrows()` vs `.to_dict()` pode ser diferença de minutos vs segundos
5. **Processamento incremental**: Reduz pico de memória sem sacrificar performance
6. **Separação de responsabilidades**: `main.py` (orquestração) vs `processamento_dados.py` (lógica)

---

## ANEXO B: Testes e Tratamentos

### Testes realizados

- ✅ Execução com dados reais dos últimos 3 trimestres
- ✅ Validação de CNPJs (testei com CNPJs válidos e inválidos)
- ✅ Verificação de integridade após joins (número de linhas preservado)
- ✅ Testes com diferentes formatos (CSV, XLSX), não encontrei um arquivo TXT
- ✅ Verificação de arquivos gerados (todos os CSVs esperados foram criados)

### Tratamento de Inconsistências

#### CNPJs Inválidos

**Decisão**: **Validar todos os CNPJs** com algoritmo de módulo 11 e **segregar inválidos** em arquivo separado.

**Justificativa**:
- CNPJs inválidos indicam problemas na qualidade dos dados da fonte
- Manter inválidos no arquivo principal polui análises
- Arquivo separado permite auditoria e correção manual

**Resultados**:
- `cnpjs_invalidos.csv`: CNPJs que falharam na validação
- Inclui: CNPJ, REG_ANS, RazaoSocial para rastreabilidade
- Arquivo principal contém **apenas CNPJs válidos**

#### CNPJs Duplicados com Razões Sociais Diferentes

**Problema**: Mesmo CNPJ pode aparecer com nomes diferentes (erro de cadastro, mudança de razão social, etc).

**Decisão**: **Detectar e reportar**, mas **manter todos os registros** no consolidado.

**Justificativa**:
- Não podemos decidir automaticamente qual nome está correto
- Remover seria perda de informação
- Relatório separado permite investigação manual

**Resultados**:
- `cnpjs_duplicados.csv`: Pares de razões sociais diferentes para o mesmo CNPJ

#### Operadoras com Dados Inconsistentes

**Problema**: Cadastro de operadoras pode ter CNPJs duplicados com RegistroANS, Modalidade ou UF diferentes.
Em healthtech dados inconsistentes viram erro regulatório. Séria atenção aqui.

**Decisão**: **Detectar inconsistências** antes do join e **usar apenas o primeiro registro** (keep='first').

**Justificativa**:
- Join com duplicatas multiplica linhas (inflação de dados)
- Usar `keep='first'` é determinístico e previsível
- Relatório de inconsistências permite correção na fonte

**Resultados**:
- `operadoras_inconsistencias.csv`: CNPJs com dados conflitantes no cadastro
- Colunas marcadas com `[INCONSISTENTE]` para fácil identificação

#### Erros de Precisão de Ponto Flutuante

**Problema**: Valores monetários podem ter erros como "759558,3999999994" devido à precisão float.

**Decisão**: **Arredondar para 2 casas decimais** em todas as etapas.

**Justificativa**:
- Valores monetários no Brasil usam 2 casas decimais
- Arredondamento elimina artefatos de precisão flutuante
- Aplicado após conversão e após agregações

#### Estratégia de Join

Para correlacionar despesas da ANS com dados cadastrais das operadoras, foi adotada uma **estratégia de join em duas etapas**:

- **Inner join por `REG_ANS`** para vincular despesas a operadoras válidas e obter o CNPJ.
- **Left join por `CNPJ`** após a consolidação, preservando todas as linhas e evitando duplicações.

Essa abordagem garante integridade referencial, evita multiplicação de registros e mantém o dataset final consistente.

#### Consolidação e Agregação

**Problema**: Empresas podem ter múltiplas despesas no mesmo trimestre (diferentes tipos de despesa).

**Decisão**: **Agregar por (CNPJ, RazaoSocial, Trimestre, Ano)** usando `.groupby().agg()`.

**Justificativa**:
- Análise temporal requer **uma linha por empresa por trimestre**
- Soma de despesas mantém total correto
- Agregação no final (após validação) garante dados limpos

#### Otimizações de Performance

##### Substituição de iterrows() por to_dict('records')

**Problema**: iterrows() tem overhead elevado em DataFrames grandes.

**Decisão**: Optei por uma abordagem vetorizada com .to_dict('records'), mais adequada ao volume de dados esperado.

##### Download de Operadoras Uma Única Vez

**Problema**: Baixar o mesmo arquivo de operadoras para cada trimestre é desperdício.

**Decisão**: **Baixar UMA VEZ** antes do loop de trimestres.

**Justificativa**:
- Cadastro de operadoras não muda entre trimestres
- Reduz tráfego de rede e tempo de execução
- Memória usada: ~50MB (aceitável para 3 trimestres)

#### Padrões Brasileiros

**Decisão**: Usar **separador `;` e decimal `,`** em todos os CSVs.

**Justificativa**:
- Padrão brasileiro de CSV
- Compatibilidade com Excel em português
- Facilita abertura direta por usuários finais
- Encoding `utf-8-sig` para BOM (abre corretamente no Excel)

#### Débito Técnico Consciente: Responsabilidades da Função `consolidar_dados_em_csv()`

A função `consolidar_dados_em_csv()` atualmente viola o **Single Responsibility Principle (SRP)**, executando múltiplas responsabilidades:

1. Validação de CNPJs (chamada a `filtrar_cnpjs_invalidos()`)
2. Agregação de despesas por trimestre (groupby + sum/mean)
3. Cálculo de estatísticas (média trimestral, desvio padrão)
4. Salvamento de múltiplos relatórios (consolidado, duplicados, desvio padrão)
5. Tratamento de erros e logging

**Justificativa da decisão atual**:
- ✅ **Escopo controlado**: Projeto CLI focado em processamento batch
- ✅ **Funcionalidade estável**: Sistema atende requisitos e funciona corretamente
- ✅ **Risco de regressão**: Refatoração prematura pode introduzir bugs
- ✅ **Prioridade de entrega**: Tempo focado em features críticas (validação de CNPJs, joins, inconsistências)

**Decisão estratégica**: Por ora, opto por **não mexer no que está funcionando**. O código está bem documentado, testado manualmente e atende ao propósito do projeto.

**Refatoração futura (se necessário)**:
Se o projeto fosse crescer em complexidade ou requisitos, consideraria decomposição em:

```python
def consolidar_dados_em_csv(dados, caminho):
    # 1. Validação
    dados_validos = filtrar_cnpjs_invalidos(dados, caminho)
    
    # 2. Agregação
    df_consolidado = agregar_despesas_por_trimestre(dados_validos)
    
    # 3. Cálculos estatísticos
    adicionar_estatisticas(df_consolidado)  # média, desvio
    
    # 4. Relatórios auxiliares
    gerar_relatorios_analise(dados_validos, df_consolidado, caminho)
    
    # 5. Salvamento principal
    salvar_arquivo_consolidado(df_consolidado, caminho)
```

**Trade-off aceito**: Acoplamento moderado em troca de simplicidade imediata e menor risco de bugs.

#### Desvio Padrão: Por Que Sem Constraint Automático?

**Decisão**: O arquivo `*_desvio_padrao.csv` é gerado **sem filtros automáticos** (sem threshold mínimo ou máximo).

**Justificativa**:

1. **Falta de contexto de negócio**: Não tenho informações sobre qual nível de volatilidade é "aceitável" para operadoras de plano de saúde
   - DP baixo pode indicar: operadora estável ✅ ou dados incompletos ❌
   - DP alto pode indicar: fraude/anomalia ⚠️ ou crescimento legítimo ✅

2. **Decisão manual é mais segura**: Em healthcare, decisões automáticas podem ter consequências regulatórias (ao meu humilde entender)
   - Eu (analista humano) devo decidir qual é o threshold apropriado, o que foi definido apenas subjetivamente em projeto
   - Não deixo a máquina descartar dados automaticamente

---

## ANEXO C: Tecnologias e Dependências

### Tecnologias Utilizadas

- **Python 3.11.x**
- **Pandas**: Manipulação e análise de dados
- **Requests**: Download de arquivos HTTP
- **BeautifulSoup4**: Parsing de HTML (listagem de diretórios)
- **zipfile**: Extração de arquivos compactados
- **tempfile**: Gerenciamento de arquivos temporários

### Dependências

```bash
pip install pandas requests beautifulsoup4 openpyxl
```

---
## Nota

A não implementação de **Banco de Dados e Análise** e **API e Interface Web** foi uma decisão consciente e técnica: optei por não desenvolver soluções em áreas nas quais ainda não possuo base prática suficiente 
para garantir um resultado correto, sustentável e defensável em uma possível avaliação técnica.

Ainda assim, o projeto foi estruturado de forma a permitir evolução natural, com separação clara de responsabilidades, funções puras e pontos de integração bem definidos, o que viabilizaria: 
- Persistência dos dados consolidados em um banco relacional (ex.: PostgreSQL)
- Exposição dos dados finais via API REST
- Posterior consumo por uma interface web.

O foco desta entrega foi garantir uma base sólida de coleta, normalização e consolidação de dados públicos da ANS, priorizando qualidade de dados, rastreabilidade, tratamento de inconsistências reais e clareza de código e documentação.

---

## Referências

- [Dados Abertos ANS](https://dadosabertos.ans.gov.br/)
- [Algoritmo de validação de CNPJ](https://www.receita.fazenda.gov.br/)
- [Documentação Pandas](https://pandas.pydata.org/docs/)

---
