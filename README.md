# contabil-cli

Sistema de consolidação e análise de demonstrações contábeis de operadoras de planos de saúde no Brasil.

## 📋 Descrição do Projeto

Este projeto extrai, processa e consolida dados de demonstrações contábeis de operadoras de planos de saúde disponibilizados pela ANS (Agência Nacional de Saúde Suplementar), correlacionando-os com informações cadastrais das operadoras.

O sistema baixa automaticamente os arquivos mais recentes (últimos 3 trimestres) do repositório de dados abertos da ANS, processa as despesas, valida CNPJs, identifica inconsistências e gera relatórios consolidados prontos para análise.

## 🎯 Funcionalidades

- ✅ Download automático de dados da ANS (FTP público)
- ✅ Processamento de múltiplos formatos (CSV, TXT, XLSX)
- ✅ Validação de CNPJs com algoritmo de módulo 11
- ✅ Detecção de inconsistências (CNPJs duplicados, dados conflitantes)
- ✅ Consolidação de dados trimestrais
- ✅ Join estruturado com cadastro de operadoras
- ✅ Geração de relatórios em formato brasileiro (sep=';', decimal=',')

## 📊 Arquivos Gerados

O sistema gera os seguintes arquivos na pasta `dados_consolidados/`:

1. **dados_processados_com_operadoras.csv** - Arquivo principal com todos os dados consolidados
2. **demonstracoes_contabeis_consolidadas.csv** - Dados consolidados antes do join com operadoras
3. **demonstracoes_contabeis_consolidadas_cnpjs_invalidos.csv** - CNPJs que falharam na validação
4. **demonstracoes_contabeis_consolidadas_cnpjs_duplicados.csv** - CNPJs com múltiplas razões sociais
5. **demonstracoes_contabeis_consolidadas_operadoras_inconsistencias.csv** - Operadoras com dados conflitantes

## 🏗️ Estrutura do Projeto

```
contabil-cli/
├── src/
│   ├── main.py                    # Orquestrador principal do sistema
│   └── processamento_dados.py     # Funções de processamento e transformação
├── dados_consolidados/            # Diretório de saída dos relatórios
└── README.md                      # Documentação do projeto
```

## 🚀 Como Executar

```powershell
# Navegar até o diretório do projeto
cd "c:\Users\moonpie\Documents\Git Projects\contabil-cli"

# Executar o script principal
python src/main.py
```

## 📚 Decisões de Arquitetura e Implementação

### 1. Estratégia de Processamento: In-Memory vs Incremental

**Decisão**: Processamento **incremental por trimestre**, mas **totalmente em memória** dentro de cada trimestre.

**Justificativa**:
- Cada arquivo ZIP trimestral contém entre 1-5 arquivos (CSV/TXT/XLSX) com ~200-300MB descompactados
- Total estimado para 3 trimestres: **~600MB-1.5GB** de dados descompactados
- Processamento incremental **por trimestre** reduz pico de memória
- Processamento **em memória dentro de cada trimestre** mantém performance (sem I/O múltiplo)
- Uso de `TemporaryDirectory`: arquivos são **processados e descartados automaticamente**
- DataFrames do Pandas são eficientes para esse volume (<2GB)

**Implementação**:
```python
# Loop processa um trimestre por vez
for arquivo in arquivos_trimestres:
    resultados = extrair_e_processar_zip(url_arquivo, df_operadoras)
    # Processa tudo em memória, depois descarta
    dados_processados.extend(resultados)
```

### 2. Resiliência a Variações de Estrutura

**Problema**: Diferentes trimestres e anos podem ter:
- Nomes de colunas diferentes (`REG_ANS` vs `REGISTRO_OPERADORA` vs `CD_REGISTRO_ANS`)
- Formatos de arquivo variados (CSV com `;`, TXT com `\t`, XLSX)
- Estruturas de diretório inconsistentes

**Decisão**: **Busca dinâmica de colunas** + **Suporte multi-formato**.

**Justificativa**:
- Hardcoded column names quebram com mudanças na fonte de dados
- Cada formato tem características específicas (encoding, separadores)
- Validação antes de processar evita falhas silenciosas

**Implementação**:
```python
# Busca dinâmica por colunas
possiveis_nomes_despesas = ['REGISTRO_OPERADORA', 'REG_ANS', 'CD_REGISTRO_ANS', ...]
for nome in possiveis_nomes_despesas:
    if nome in df_despesas.columns:
        coluna_reg_ans_despesas = nome
        break

# Suporte multi-formato
if caminho_arquivo.endswith('.csv'):
    df = pd.read_csv(caminho_arquivo, encoding='latin1', sep=';')
elif caminho_arquivo.endswith('.txt'):
    df = pd.read_csv(caminho_arquivo, encoding='latin1', sep='\t')
elif caminho_arquivo.endswith('.xlsx'):
    df = pd.read_excel(caminho_arquivo)
```

**Benefícios**:
- ✅ Sistema continua funcionando mesmo com mudanças na nomenclatura da ANS
- ✅ Suporta qualquer formato encontrado nos arquivos ZIP
- ✅ Mensagens de debug mostram colunas disponíveis quando algo falha

### 3. Tratamento de Inconsistências

#### 3.1 CNPJs Inválidos

**Decisão**: **Validar todos os CNPJs** com algoritmo de módulo 11 e **segregar inválidos** em arquivo separado.

**Justificativa**:
- CNPJs inválidos indicam problemas na qualidade dos dados da fonte
- Manter inválidos no arquivo principal polui análises
- Arquivo separado permite auditoria e correção manual

**Implementação**:
```python
def validar_cnpj(cnpj: str) -> bool:
    # Remove caracteres não numéricos
    cnpj_numeros = re.sub(r'\D', '', str(cnpj))
    
    # Verifica 14 dígitos
    if len(cnpj_numeros) != 14:
        return False
    
    # Calcula dígitos verificadores (módulo 11)
    # ... algoritmo completo no código
```

**Resultados**:
- 📄 `cnpjs_invalidos.csv`: CNPJs que falharam na validação
- Inclui: CNPJ, REG_ANS, RazaoSocial para rastreabilidade
- Arquivo principal contém **apenas CNPJs válidos**

#### 3.2 CNPJs Duplicados com Razões Sociais Diferentes

**Problema**: Mesmo CNPJ pode aparecer com nomes diferentes (erro de cadastro, mudança de razão social, etc).

**Decisão**: **Detectar e reportar**, mas **manter todos os registros** no consolidado.

**Justificativa**:
- Não podemos decidir automaticamente qual nome está correto
- Remover seria perda de informação
- Relatório separado permite investigação manual

**Implementação**:
```python
# Agrupa por CNPJ e verifica se há razões sociais únicas > 1
duplicados = df.groupby('CNPJ').filter(lambda x: x['RazaoSocial'].nunique() > 1)
```

**Resultados**:
- 📄 `cnpjs_duplicados.csv`: Pares de razões sociais diferentes para o mesmo CNPJ

#### 3.3 Operadoras com Dados Inconsistentes

**Problema**: Cadastro de operadoras pode ter CNPJs duplicados com RegistroANS, Modalidade ou UF diferentes.
Em healthtech dados inconsistentes viram erro regulatório. Séria atenção aqui.

**Decisão**: **Detectar inconsistências** antes do join e **usar apenas o primeiro registro** (keep='first').

**Justificativa**:
- Join com duplicatas multiplica linhas (inflação de dados)
- Usar `keep='first'` é determinístico e previsível
- Relatório de inconsistências permite correção na fonte

**Implementação**:
```python
# Detecta e salva inconsistências
salvar_operadoras_duplicadas_com_inconsistencias(df_operadoras, caminho)

# Remove duplicatas antes do join
df_operadoras_unicas = df_operadoras.drop_duplicates(subset=['CNPJ'], keep='first')
```

**Resultados**:
- 📄 `operadoras_inconsistencias.csv`: CNPJs com dados conflitantes no cadastro
- Colunas marcadas com `[INCONSISTENTE]` para fácil identificação

#### 3.4 Erros de Precisão de Ponto Flutuante

**Problema**: Valores monetários podem ter erros como "759558,3999999994" devido à precisão float.

**Decisão**: **Arredondar para 2 casas decimais** em todas as etapas.

**Justificativa**:
- Valores monetários no Brasil usam 2 casas decimais
- Arredondamento elimina artefatos de precisão flutuante
- Aplicado após conversão e após agregações

**Implementação**:
```python
# Converte e arredonda
df_filtered[col] = pd.to_numeric(
    df_filtered[col].astype(str).str.replace(',', '.'),
    errors='coerce'
).round(2)

# Arredonda após agregação
df_consolidado['ValorDespesas'] = df_consolidado['ValorDespesas'].round(2)
```

### 4. Estratégia de Join

**Problema**: Correlacionar despesas (por REG_ANS) com operadoras (CNPJ + cadastro).

**Decisão**: **Join em duas etapas**:
1. **Inner join** Despesas × Operadoras (por REG_ANS) → adiciona CNPJ
2. **Left join** Consolidado × Operadoras (por CNPJ) → adiciona RegistroANS, Modalidade, UF

**Justificativa**:

**Primeira etapa (Inner Join)**:
- Garante que só processamos despesas de operadoras conhecidas
- Adiciona CNPJ necessário para consolidação
- Filtra dados órfãos (sem operadora correspondente)

**Segunda etapa (Left Join)**:
- Mantém **todas as linhas** do arquivo consolidado (sem multiplicação)
- Adiciona apenas colunas necessárias: RegistroANS, Modalidade, UF
- Registros sem match recebem "Registro sem match no cadastro"

**Por que não um único join?**
- Primeiro join é por REG_ANS (chave primária em despesas)
- Segundo join é por CNPJ (chave única após consolidação)
- Separação mantém integridade referencial e evita duplicatas

**Implementação**:
```python
# Etapa 1: Despesas → Operadoras (inner join por REG_ANS)
df_merged = df_despesas.merge(
    df_operadoras[[coluna_reg_ans_operadoras, coluna_cnpj, coluna_razao]],
    left_on=coluna_reg_ans_despesas,
    right_on=coluna_reg_ans_operadoras,
    how='inner'
)

# Etapa 2: Consolidado → Operadoras (left join por CNPJ)
df_resultado = df_dados.merge(
    df_operadoras_filtradas,
    on='CNPJ',
    how='left'
)
```

**Validação**:
```python
# Verificação simples para detectar efeitos colaterais inesperados durante o join
if num_linhas_resultado != num_linhas_original:
    raise ValueError(f"Join produziu {num_linhas_resultado} linhas em vez de {num_linhas_original}")
```

### 5. Consolidação e Agregação

**Problema**: Empresas podem ter múltiplas despesas no mesmo trimestre (diferentes tipos de despesa).

**Decisão**: **Agregar por (CNPJ, RazaoSocial, Trimestre, Ano)** usando `.groupby().agg()`.

**Justificativa**:
- Análise temporal requer **uma linha por empresa por trimestre**
- Soma de despesas mantém total correto
- Agregação no final (após validação) garante dados limpos

**Implementação**:
```python
df_consolidado = df_consolidado.groupby(
    ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano'], 
    as_index=False
).agg({
    'ValorDespesas': 'sum'
})
```

### 6. Otimizações de Performance

#### 6.1 Substituição de iterrows() por to_dict('records')

**Problema**: iterrows() tem overhead elevado em DataFrames grandes.

**Decisão**: Optei por uma abordagem vetorizada com .to_dict('records'), mais adequada ao volume de dados esperado.

**Implementação**:
```python
# ANTES (lento)
for _, row in df_merged.iterrows():
    resultados.append({...})

# DEPOIS (rápido)
df_resultado = df_merged[['col1', 'col2', ...]].copy()
df_resultado.columns = ['new1', 'new2', ...]
resultados = df_resultado.to_dict('records')
```

#### 6.2 Download de Operadoras Uma Única Vez

**Problema**: Baixar o mesmo arquivo de operadoras para cada trimestre é desperdício.

**Decisão**: **Baixar UMA VEZ** antes do loop de trimestres.

**Justificativa**:
- Cadastro de operadoras não muda entre trimestres
- Reduz tráfego de rede e tempo de execução
- Memória usada: ~50MB (aceitável para 3 trimestres)

**Implementação**:
```python
# Baixar operadoras UMA VEZ antes do loop
df_operadoras = pd.read_csv(caminho_operadoras, encoding='latin1', sep=';')

# Reusar em todos os trimestres
for arquivo in arquivos_trimestres:
    resultados = extrair_e_processar_zip(url_arquivo, df_operadoras)
```

### 7. Padrões Brasileiros

**Decisão**: Usar **separador `;` e decimal `,`** em todos os CSVs.

**Justificativa**:
- Padrão brasileiro de CSV
- Compatibilidade com Excel em português
- Facilita abertura direta por usuários finais
- Encoding `utf-8-sig` para BOM (abre corretamente no Excel)

**Implementação**:
```python
df.to_csv(
    caminho_saida, 
    index=False, 
    encoding='utf-8-sig',  # BOM para Excel
    sep=';',               # Separador brasileiro
    decimal=','            # Decimal brasileiro
)
```

## 🔧 Tecnologias Utilizadas

- **Python 3.x**
- **Pandas**: Manipulação e análise de dados
- **Requests**: Download de arquivos HTTP
- **BeautifulSoup4**: Parsing de HTML (listagem de diretórios)
- **zipfile**: Extração de arquivos compactados
- **tempfile**: Gerenciamento de arquivos temporários

## 📦 Dependências

```bash
pip install pandas requests beautifulsoup4 openpyxl
```

## 🎓 Lições Aprendidas

1. **Sempre validar dados externos**: CNPJs inválidos e duplicatas são comuns em bases públicas
2. **Busca dinâmica de colunas**: Hardcoded names quebram quando a fonte muda
3. **Agregação é essencial**: Dados brutos raramente estão no formato ideal para análise
4. **Performance importa**: `iterrows()` vs `.to_dict()` pode ser diferença de minutos vs segundos
5. **Processamento incremental**: Reduz pico de memória sem sacrificar performance
6. **Separação de responsabilidades**: `main.py` (orquestração) vs `processamento_dados.py` (lógica)

## 🐛 Tratamento de Erros

O sistema implementa múltiplas camadas de validação:

- ✅ Verificação de status HTTP (200) antes de processar
- ✅ Try-except em todas as funções críticas (foi praticamente uma forma de debugar)
- ✅ Validação de existência de colunas antes de usar
- ✅ Verificação de integridade após joins
- ✅ Mensagens de debug detalhadas em cada etapa

## 📈 Próximos Passos

- [ ] Adicionar testes unitários (pytest)
- [ ] Implementar logging estruturado (logging module)
- [ ] Criar CLI com argumentos (argparse)

## 📄 Licença

Este projeto foi desenvolvido para fins educacionais e de análise de dados públicos.

## 👤 Autor

**moonpie**  
Projeto: contabil-cli  
Data: Janeiro 2026

---

**Dados fornecidos por**: [ANS - Agência Nacional de Saúde Suplementar](https://dadosabertos.ans.gov.br/)