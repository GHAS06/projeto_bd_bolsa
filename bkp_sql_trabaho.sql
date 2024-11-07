-- Estrutura da Tabela pagamentos do banco de dados atividade_bolsa

CREATE TABLE IF NOT EXISTS pagamentos (
    data_referencia VARCHAR(100),
    data_competencia VARCHAR(100),
    uf VARCHAR(5),
    codigo_municipio VARCHAR(100),
    nome_municipio VARCHAR(100),
    cpf_beneficiario VARCHAR(100),
    nis_beneficiario VARCHAR(100),
    nome_beneficiario VARCHAR(100),
    valor_parcela DECIMAL(10, 2)
);
/*
	Drop caso dê erros de importação de dados
	DROP TABLE pagamento
*/
-- contabilizando o total de registros que existem na tabela
SELECT COUNT(valor_parcela)  FROM pagamentos;

/* Forma padrão de contabilizar valores null de uma tabela*/
SELECT COUNT(cpf_beneficiario) AS cpf_null FROM pagamentos AS p 
WHERE p.cpf_beneficiario IS NULL;
-- Aqui retornou 0 por que os dados possuem strings vazias, valores invisivéis 

/* 
Contabilizando a quantidadae de cpf com valores null na tabela pagamentos,
com valores vazios/invisivéis.
*/

SELECT COUNT(*) AS empty_or_null_cpf
FROM pagamentos 
WHERE cpf_beneficiario IS NULL OR cpf_beneficiario = '' OR cpf_beneficiario ~ '^\s*$';

/* Selecionando todos os dados da tabela pagamentos*/
SELECT * FROM pagamentos;

/*Descobrindo o nome de municípios e seus respectivos UF*/

SELECT DISTINCT(nome_municipio), uf FROM pagamentos WHERE uf IN('DF','SP','BA') GROUP BY nome_municipio, uf;


/*DESCOBRINDO O NOME DOS MUNICIPIOS*/

SELECT DISTINCT(nome_municipio) FROM pagamentos WHERE uf = 'DF' GROUP BY nome_municipio;
SELECT DISTINCT(nome_municipio) FROM pagamentos WHERE uf = 'SP' GROUP BY nome_municipio;
SELECT DISTINCT(nome_municipio) FROM pagamentos WHERE uf = 'BA' GROUP BY nome_municipio;

/*Selecionando o total de valores pagos no mês referente de março*/
SELECT SUM(valor_parcela) FROM pagamentos;
-- valor total = 13.373.651.126.00, retornou esse cálculo em 12.86s 

/*Selecionando o maior registro de valor_pacela do bolsa família em ordem decrecente */
SELECT * FROM pagamentos ORDER BY valor_parcela DESC LIMIT 1000;

/*Quantidade de pessoas que receberam bolsa família referente ao mês de março em São Paulo*/
SELECT COUNT(*) FROM pagamentos WHERE nome_municipio = 'SAO PAULO';
-- retornou 676.088 dados em 15.11s

/*Quantidades de pessoa quer receberam bolsa família referente ao mês de março em Brasilia*/
SELECT COUNT(*) FROM pagamentos WHERE nome_municipio = 'BRASILIA';
-- retornou 184.844 dados em 14.82s

/*Quantidade de pessoas que receberam bolsa família referente ao mês de março em Salvador */
SELECT COUNT(*) FROM pagamentos WHERE nome_municipio = 'SALVADOR';
-- retornou 292.214 dados em 14.72s

/*Retorna a soma total de parcela por munícipio, ordenado em ordem alfabetica */
SELECT nome_municipio, uf, SUM(valor_parcela) AS total_valor_pago
FROM pagamentos
GROUP BY nome_municipio, uf
ORDER BY nome_municipio;
-- Esse cálculo retornou em 32.99s

/* Retorna os municipios que mais receberam */
SELECT nome_municipio, uf, SUM(valor_parcela) AS total_valor_pago
FROM pagamentos
GROUP BY nome_municipio, uf
ORDER BY total_valor_pago DESC;
-- esse calculo retornou em 28.44s

/* Retorna  uma lista de cpf repetidos */
SELECT cpf_beneficiario, COUNT(*) AS quantidade
FROM pagamentos
GROUP BY cpf_beneficiario
HAVING COUNT(*) > 1
ORDER BY quantidade DESC;
-- Retornou resultado em 1 min e 51s, retornou 3.515.664 de valores null

