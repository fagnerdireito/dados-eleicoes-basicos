agora crie um novo arquivo python isolado para criar uma tabela unificada de todos os csv dentro de bweb.
o cabecalho deles é 
2024:
"DT_GERACAO";"HH_GERACAO";"ANO_ELEICAO";"CD_TIPO_ELEICAO";"NM_TIPO_ELEICAO";"CD_PLEITO";"DT_PLEITO";"NR_TURNO";"CD_ELEICAO";"DS_ELEICAO";"SG_UF";"CD_MUNICIPIO";"NM_MUNICIPIO";"NR_ZONA";"NR_SECAO";"NR_LOCAL_VOTACAO";"CD_CARGO_PERGUNTA";"DS_CARGO_PERGUNTA";"NR_PARTIDO";"SG_PARTIDO";"NM_PARTIDO";"DT_BU_RECEBIDO";"QT_APTOS";"QT_COMPARECIMENTO";"QT_ABSTENCOES";"CD_TIPO_URNA";"DS_TIPO_URNA";"CD_TIPO_VOTAVEL";"DS_TIPO_VOTAVEL";"NR_VOTAVEL";"NM_VOTAVEL";"QT_VOTOS";"NR_URNA_EFETIVADA";"CD_CARGA_1_URNA_EFETIVADA";"CD_CARGA_2_URNA_EFETIVADA";"CD_FLASHCARD_URNA_EFETIVADA";"DT_CARGA_URNA_EFETIVADA";"DS_CARGO_PERGUNTA_SECAO";"DS_SECOES_AGREGADAS";"DT_ABERTURA";"DT_ENCERRAMENTO";"QT_ELEI_BIOM_SEM_HABILITACAO";"DT_EMISSAO_BU";"NR_JUNTA_APURADORA";"NR_TURMA_APURADORA".
2022:
"DT_GERACAO";"HH_GERACAO";"ANO_ELEICAO";"CD_TIPO_ELEICAO";"NM_TIPO_ELEICAO";"CD_PLEITO";"DT_PLEITO";"NR_TURNO";"CD_ELEICAO";"DS_ELEICAO";"SG_UF";"CD_MUNICIPIO";"NM_MUNICIPIO";"NR_ZONA";"NR_SECAO";"NR_LOCAL_VOTACAO";"CD_CARGO_PERGUNTA";"DS_CARGO_PERGUNTA";"NR_PARTIDO";"SG_PARTIDO";"NM_PARTIDO";"DT_BU_RECEBIDO";"QT_APTOS";"QT_COMPARECIMENTO";"QT_ABSTENCOES";"CD_TIPO_URNA";"DS_TIPO_URNA";"CD_TIPO_VOTAVEL";"DS_TIPO_VOTAVEL";"NR_VOTAVEL";"NM_VOTAVEL";"QT_VOTOS";"NR_URNA_EFETIVADA";"CD_CARGA_1_URNA_EFETIVADA";"CD_CARGA_2_URNA_EFETIVADA";"CD_FLASHCARD_URNA_EFETIVADA";"DT_CARGA_URNA_EFETIVADA";"DS_CARGO_PERGUNTA_SECAO";"DS_AGREGADAS";"DT_ABERTURA";"DT_ENCERRAMENTO";"QT_ELEITORES_BIOMETRIA_NH";"DT_EMISSAO_BU";"NR_JUNTA_APURADORA";"NR_TURMA_APURADORA"

Tem 2 campos com nomes diferentes entre 2022 e 2024:
aqui esta o de para.
DS_AGREGADAS -> DS_SECOES_AGREGADAS
QT_ELEITORES_BIOMETRIA_NH -> QT_ELEI_BIOM_SEM_HABILITACAO
unifique os nomes dos campos para o mesmo nome de 2024.
esse script python vai criar uma tabela unica com todos os dados destes boletim de urna.
o nome da tabela vai ser boletim_urna.

# Regras de Contexto
- language: python
- Database: MySQL
- os arquivos csv sempre estao com codificacao latin1 

### Database Configuration (Development)
Sempre considere as seguintes variáveis de conexão para operações de banco de dados:
* **Connection:** `mysql`
* **Host:** `127.0.0.1` (localhost)
* **Port:** `3306`
* **Database:** `eleicoes`
* **User:** `root`
* **Password:** (empty)














agora crie um novo arquivo python isolado para criar uma tabela unificada de todos os csv dentro de /Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/dados/consulta_cand_*
pattern: consulta_cand_{ano}
o cabecalho deles é 
2024:
"DT_GERACAO";"HH_GERACAO";"ANO_ELEICAO";"CD_TIPO_ELEICAO";"NM_TIPO_ELEICAO";"NR_TURNO";"CD_ELEICAO";"DS_ELEICAO";"DT_ELEICAO";"TP_ABRANGENCIA";"SG_UF";"SG_UE";"NM_UE";"CD_CARGO";"DS_CARGO";"SQ_CANDIDATO";"NR_CANDIDATO";"NM_CANDIDATO";"NM_URNA_CANDIDATO";"NM_SOCIAL_CANDIDATO";"NR_CPF_CANDIDATO";"DS_EMAIL";"CD_SITUACAO_CANDIDATURA";"DS_SITUACAO_CANDIDATURA";"TP_AGREMIACAO";"NR_PARTIDO";"SG_PARTIDO";"NM_PARTIDO";"NR_FEDERACAO";"NM_FEDERACAO";"SG_FEDERACAO";"DS_COMPOSICAO_FEDERACAO";"SQ_COLIGACAO";"NM_COLIGACAO";"DS_COMPOSICAO_COLIGACAO";"SG_UF_NASCIMENTO";"DT_NASCIMENTO";"NR_TITULO_ELEITORAL_CANDIDATO";"CD_GENERO";"DS_GENERO";"CD_GRAU_INSTRUCAO";"DS_GRAU_INSTRUCAO";"CD_ESTADO_CIVIL";"DS_ESTADO_CIVIL";"CD_COR_RACA";"DS_COR_RACA";"CD_OCUPACAO";"DS_OCUPACAO";"CD_SIT_TOT_TURNO";"DS_SIT_TOT_TURNO"

esse script python vai criar uma tabela unica com todos os dados destes consulta_cand.
o nome da tabela vai ser consulta_cand.

observe o arquivo import_boletim_urna.py como referencia.
crie um indice para nao permitir duplicidade de dados.

# Regras de Contexto
- language: python
- Database: MySQL
- os arquivos csv sempre estao com codificacao latin1 

### Database Configuration (Development)
Sempre considere as seguintes variáveis de conexão para operações de banco de dados que estao no arquivo .env:
* **Connection:** `mysql`
* **Host:** `127.0.0.1` (localhost)
* **Port:** `3306`
* **Database:** `eleicoes`
* **User:** `root`
* **Password:** (empty)



















agora crie um novo arquivo python isolado para criar uma tabela unificada de todos os csv dentro de /Users/fagnerdossgoncalves/wwwroot/lab/dados-eleicoes-basicos/dados/consulta_vagas_*
pattern: consulta_vagas_{ano}
o cabecalho deles é 
2024:
"DT_GERACAO";"HH_GERACAO";"ANO_ELEICAO";"CD_TIPO_ELEICAO";"NM_TIPO_ELEICAO";"CD_ELEICAO";"DS_ELEICAO";"DT_ELEICAO";"DT_POSSE";"SG_UF";"SG_UE";"NM_UE";"CD_CARGO";"DS_CARGO";"QT_VAGA"

esse script python vai criar uma tabela unica com todos os dados destes consulta_vagas.
o nome da tabela vai ser consulta_vagas.

observe o arquivo import_consulta_cand.py como referencia.
crie um indice para nao permitir duplicidade de dados.

# Regras de Contexto
- language: python
- Database: MySQL
- os arquivos csv sempre estao com codificacao latin1 

### Database Configuration (Development)
Sempre considere as seguintes variáveis de conexão para operações de banco de dados que estao no arquivo .env:
* **Connection:** `mysql`
* **Host:** `127.0.0.1` (localhost)
* **Port:** `3306`
* **Database:** `eleicoes`
* **User:** `root`
* **Password:** (empty)








agora crie um novo arquivo python isolado para criar uma view mysql com o total de votos por candidato por municipio por ano.
exemplo como era no bigquery:
SELECT
    MAX(cc.NM_URNA_CANDIDATO) AS NM_URNA_CANDIDATO,
    bu.NM_VOTAVEL,
    SUM(bu.QT_VOTOS) AS total_votos,
    MAX(bu.ANO_ELEICAO) AS ANO_ELEICAO,
    MAX(bu.NM_MUNICIPIO) AS NM_MUNICIPIO,
    MAX(bu.CD_MUNICIPIO) AS CD_MUNICIPIO,
    bu.CD_ELEICAO,
    bu.NR_TURNO,
    MAX(bu.SG_UF) AS SG_UF,
    MAX(bu.DS_CARGO_PERGUNTA) AS DS_CARGO_PERGUNTA,
    MAX(cc.SG_PARTIDO) AS SG_PARTIDO,
    MAX(cc.DS_SIT_TOT_TURNO) AS SITUACAO_ELEICAO
FROM
    `elegis-1262.eleicoes2024.boletim_urna` AS bu
INNER JOIN
    `elegis-1262.eleicoes2024.consulta_cand` AS cc
    ON bu.ANO_ELEICAO = cc.ANO_ELEICAO
    AND bu.NR_VOTAVEL = cc.NR_CANDIDATO
    AND bu.SG_UF = cc.SG_UF
    AND bu.CD_CARGO_PERGUNTA = cc.CD_CARGO
    AND CAST(bu.CD_MUNICIPIO AS STRING) = CAST(cc.SG_UE AS STRING)
GROUP BY
    bu.CD_ELEICAO,
    bu.NR_TURNO,
    bu.NM_VOTAVEL
ORDER BY
    total_votos DESC

- mas agora implemente em formato mysql.











SELECT
        MAX(cc.NM_URNA_CANDIDATO) AS NM_URNA_CANDIDATO,
        bu.NM_VOTAVEL,
        SUM(CAST(bu.QT_VOTOS AS UNSIGNED)) AS total_votos,
        bu.ANO_ELEICAO,
        bu.NM_MUNICIPIO,
        bu.CD_MUNICIPIO,
        bu.CD_ELEICAO,
        bu.NR_TURNO,
        bu.SG_UF,
        MAX(bu.DS_CARGO_PERGUNTA) AS DS_CARGO_PERGUNTA,
        MAX(cc.SG_PARTIDO) AS SG_PARTIDO,
        MAX(cc.DS_SIT_TOT_TURNO) AS SITUACAO_ELEICAO
    FROM
        boletim_urna AS bu
    LEFT JOIN
        consulta_cand AS cc
        ON bu.ANO_ELEICAO = cc.ANO_ELEICAO
        AND bu.NR_VOTAVEL = cc.NR_CANDIDATO
        AND bu.SG_UF = cc.SG_UF
        AND bu.CD_CARGO_PERGUNTA = cc.CD_CARGO
        AND bu.CD_MUNICIPIO = cc.SG_UE
    GROUP BY
        bu.ANO_ELEICAO,
        bu.CD_MUNICIPIO,
        bu.NM_MUNICIPIO,
        bu.CD_ELEICAO,
        bu.NR_TURNO,
        bu.SG_UF,
        bu.NR_VOTAVEL
    ORDER BY
        total_votos DESC;





SELECT NR_VOTAVEL,NM_VOTAVEL, SUM(QT_VOTOS) as votos FROM boletim_urna WHERE NM_VOTAVEL='LISSAUER VIEIRA' GROUP BY NR_VOTAVEL;




ALTER TABLE boletim_urna ADD COLUMN id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;










crie um .sh bash file script para executar em sequencia estes arquivos.
python import_boletim_urna.py
python import_consulta_cand.py
python import_consulta_vagas.py
python create_table_votos_candidatos.py
python create_table_votos_partido.py







tente recriar em go o arquivo 4_create_table_votos_candidatos.go, para tornar   
  ele o mais simples possivel e sequencial sem threads  pois como se trata de      
  create table temos que ter mais cuidado, o mais importante é que ele seja sequencial e sem threads. e tem que ser um script simples que nao precise de muito codigo. e que rode em um servidor de apenas 2 nucleos vcpu. mas com consistencia. mas se tiver como adicionar threads, adicione, mas deve ser sequencial. e se tiver como deixar o mais rapido possivel, deixe.
  a prioridade é a velocidade e a capacidade de processamento em cpus de baixo custo e de baixa memoria.












Verificando o esquema da tabela votos_candidatos e uma amostra de dados — os nomes vêm de NM_URNA_CANDIDATO. uf AC, cidade bujari.
Confirmando: NM_URNA_CANDIDATO está vazio no banco; o nome aparece em NM_VOTAVEL. Verificando vereadores:
Detectado o problema: no banco NM_URNA_CANDIDATO está vazio e os nomes estão em NM_VOTAVEL (ex.: "PADEIRO"). O código só usava NM_URNA_CANDIDATO e o filtro de vereador exigia NM_URNA_CANDIDATO preenchido — por isso a lista de vereadores ficava vazia. Corrigindo mapeamento e filtro e adicionando um teste unitário simples
Detectado o problema: no banco NM_URNA_CANDIDATO está vazio e os nomes estão em NM_VOTAVEL (ex.: "PADEIRO"). O código só usava NM_URNA_CANDIDATO e o filtro de vereador exigia NM_URNA_CANDIDATO preenchido — por isso a lista de vereadores ficava vazia. Corrigindo mapeamento e filtro e adicionando um teste unitário simples.
No banco ligado à conexão eleicoes (no seu .env,  aponta para dadoseleicoes), a tabela votos_candidatos tem NM_URNA_CANDIDATO vazio para os registros de BUJARI/2024, e o nome que aparece no agregado está em NM_VOTAVEL (ex.: PADEIRO, MICHEL MARQUES).
O EleicoesPage só usava NM_URNA_CANDIDATO na tela e no filtro de vereador exigia NM_URNA_CANDIDATO preenchido. 
Prefeito: porcentagens e votos vinham certos, mas nome ia vazio.
Vereador: todos os candidatos eram descartados pelo gerando “Nenhum resultado encontrado”.

analise porque está ficando vazio. verifique se o arquivo que cria a tabela votos candidatos que se relaciona com a tabela consulta_cand está correta.
pode ser que a tabela consulta_cand nao possui a referencia, precisamos saber onde esta o erro.










Verificando o esquema da tabela votos_candidatos e uma amostra de dados — os nomes vêm de NM_URNA_CANDIDATO. uf AC, cidade bujari.
Confirmando: NM_URNA_CANDIDATO está vazio no banco; o nome aparece em NM_VOTAVEL. Verificando vereadores:
Detectado o problema: no banco NM_URNA_CANDIDATO está vazio e os nomes estão em NM_VOTAVEL (ex.: "PADEIRO"). O código só usava NM_URNA_CANDIDATO e o filtro de vereador exigia NM_URNA_CANDIDATO preenchido — por isso a lista de vereadores ficava vazia. Corrigindo mapeamento e filtro e adicionando um teste unitário simples
Detectado o problema: no banco NM_URNA_CANDIDATO está vazio e os nomes estão em NM_VOTAVEL (ex.: "PADEIRO"). O código só usava NM_URNA_CANDIDATO e o filtro de vereador exigia NM_URNA_CANDIDATO preenchido — por isso a lista de vereadores ficava vazia. Corrigindo mapeamento e filtro e adicionando um teste unitário simples.
No banco ligado à conexão eleicoes (no seu .env,  aponta para dadoseleicoes), a tabela votos_candidatos tem NM_URNA_CANDIDATO vazio para os registros de BUJARI/2024, e o nome que aparece no agregado está em NM_VOTAVEL (ex.: PADEIRO, MICHEL MARQUES).
O EleicoesPage só usava NM_URNA_CANDIDATO na tela e no filtro de vereador exigia NM_URNA_CANDIDATO preenchido. 
Prefeito: porcentagens e votos vinham certos, mas nome ia vazio.
Vereador: todos os candidatos eram descartados pelo gerando “Nenhum resultado encontrado”.

analise porque está ficando vazio. verifique se o arquivo que cria a tabela votos candidatos que se relaciona com a tabela consulta_cand está correta.
pode ser que a tabela consulta_cand nao possui a referencia, precisamos saber onde esta o erro.

apenas analise






SELECT * FROM consulta_cand WHERE "NM_UE" = 'ACRELÂNDIA' AND "ANO_ELEICAO"='2024' AND "DS_CARGO" = 'PREFEITO';



SELECT * FROM votos_candidatos WHERE "NM_MUNICIPIO" = 'ACRELÂNDIA' AND "SG_UF"='AC' AND "ANO_ELEICAO"='2024' AND "DS_CARGO_PERGUNTA" = 'PREFEITO';




# cidades
crie o arquivo 6 na pasta go_postgres/ que vai importar para o postgresql  os municipio_tse_ibge.csv com essa etrutura. se a tabela ja existe nao cria se os dados existem nao inclui. 
o nome do municipio tem que ser importado em maiuscula.


#estados
crie o arquivo 7 na pasta go_postgres/ que vai importar para o postgresql  os estados do brasil e DF com a tabela de nome estados com essa etrutura. se a tabela ja existe nao cria se os dados existem nao inclui. a origem dos dados podem ser a tabela boletim_de_urna.



dentro da pasta go_postgres adicione o arquivo run_all.sh que vai executar todos os arquivos go em sequencia do 1 ao 7.