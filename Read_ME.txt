Como proposto pela idéia inicial do desafio, esses script trabalha em 4 partes

Para executa-lo basta rodar o arquivo main.py, entretando é preciso colocar sua própria chave da api CoinCap no arquivo .env
bem como a conta de projeto do google cloud, caso queira subir os arquivos automaticamente para o drive (por medidas de segurança as chaves estão ocultas no git)


Apóes acertar as chaves de API, o próximo passo é instalar as dependencias de requirements.txt, após isso já é possível roda main.py

O arquivo api_client.py é onde a conexão e a extração de dados da CoinCap acontece, como a api tem limite de tamanha por requisição (2000) e limite de usos por ticket, optei por não trazer tantos dados, apenas os principais e mais superficiais de cada cripto 

O arquivo database_manager.py cria duas tabelas com os dados extraidos em um banco de dados offline, optei por essa alternativa por ser mais simples do que configurar um banco online do zero apenas para o projeto 

Já o arquivo google_sheets_uploader.py é o responsável por pegar as tabelas que foram estruturadas no banco a repassar para planilhas do google sheets, pois é com elas que o dashboard no looker studio é criado


por fim o link para o dashboard se encontra em https://lookerstudio.google.com/reporting/bea1393e-fdc9-4ca3-8e28-878fd9759797

