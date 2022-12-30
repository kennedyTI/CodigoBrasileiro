import pymongo
from pprint import pprint

class MongoAtlas:
    
    def __init__(self) -> None:            
        
        #   Conectando com o Banco de dados em nuvem.
        self.client = pymongo.MongoClient("COLE AQUI A URL QUE TU COPIOU. não esquece de substituir "<senha>" pela senha") 
        
        #   Retorna Bancos de Dados existentes.
        self.resposta = client.list_database_names() 
    
    def acessarBancoDeDados(self, bancoDeDado, collection=None):     
        """_summary_
        
        Função criada com a finalidade de conectar a um banco de dados em nuvem. Para usa-la é necessário passa como parâmetro os seguintes itens:
        
            1ª) Nome do Banco de dados >> OBRIGATÓRIO.
            
            2ª) Nome da collection >> OPCIONAL.
        """ 
    
        # Acessando um banco de dados específico.
        self.bancoDeDados = self.client.get_database(bancoDeDado)
        
        #   Retornando as collection existentes no banco de dados.
        self.resposta = self.bancoDeDados.list_collection_names()
        
        # Acessando uma collection específica.
        if collection:
            self.collection = self.bancoDeDados.collection
    
    """_summary_
        
        As Funções criarCollection e alterarCollection, possuem a finalidade de criar e alterar respectivamente  seus campos com suas regras de validação. 
        Para usa-la é necessário passa como parâmetro os seguintes itens:
        
        1ª) Nome da Collection.
        
        2ª) um dicionário contendo os campos e duas regras de validação. Veja o exemplo abaixo:

            dictdados = 
            {
                "stuff": 
                {
                    "bsonType": "object",
                    "required": ["teste", "testando" >> dados que serão manipulados],
                    "properties": 
                    {
                        "teste": 
                        {
                            "bsonType": "string",
                            "pattern": "^[a-z_]{3-20}$",
                            "description": "defina a mensagem de error"
                        },
                        "testando": 
                        {
                            "bsonType": "string",
                            "minLength": 2,
                            "maxLength": 30,
                            "description": "defina a mensagem de error"
                        },        
                    }
                }
            }
        """
                
    def criarCollection(self,collection, dictdados):
        """_summary_
        
        Função possui a finalidade de criar collection com seus campos e  validações. Para usa-la é necessário passar como parâmetro os seguintes itens:
        
        1ª) Nome da Collection.
        
        2ª) um dicionário contendo os campos como também, suas regras de validação. 

            OBS: Exemplo no _summary principal>
        """
        
        # Acessando uma collection.
        self.bancoDeDados.create_collection(collection, validator= {"$jsonSchema": dictdados})
    
    
    def alterarCollection(self,collection, dictdados):
        """_summary_
        
        Função criada com a finalidade de alterar validações de Collections existentes. Para usa-la é necessário passar como parâmetro os seguintes itens:
        
        1ª) Nome da Collection.
        
        2ª) um dicionário contendo os campos como também, suas regras de validação. 

            OBS: Exemplo no _summary principal>
        """
        
        # Acessando uma collection.
        self.bancoDeDados.command(collection, validator= { "$jsonSchema": dictdados})
    
    def inserindoDados(self,dados):        
        """_summary_
        
        Função criada com a finalidade de inserir dados em uma Collections existentes. Para usa-la é necessário passar como parâmetro os seguintes itens:
            
            1ª) Inserir um único dado. Veja o exemplo abaixo:

                dictDados= 
                {
                    "Nome": "Nego Do Borel",
                    "CPF": "12345678905",
                    "DataDeNascimento": date(year=1994, month=9, day=24),
                    "Sexo": "Masculino",
                    "Contatos": 
                    {
                        "Email":"dfgdg@asdd.com",
                        "Tel01": 27987654321,
                        "Tel02": 28123456789
                    },
                    "Endereço":
                    {
                        "CEP": 29145250,
                        "Num": 151,
                        "Logradouro": "Dante",
                        "Bairro": "Veneza",
                        "Cidade": "Olinda",
                        "UF": "BA",
                    }                   
                }
            
            2ª) Inserir uma coleção de dados. Veja o exemplo abaixo:

                listDados= 
                [
                    {
                        "Nome": "Nego Do Borel",
                        "CPF": "12345678905",
                        "DataDeNascimento": date(year=1994, month=9, day=24),
                        "Sexo": "Masculino",
                        "Contatos": 
                        {
                            "Email":"dfgdg@asdd.com",
                            "Tel01": 27987654321,
                            "Tel02": 28123456789
                        },
                        "Endereço":
                        {
                            "CEP": 29145250,
                            "Num": 151,
                            "Logradouro": "Dante",
                            "Bairro": "Veneza",
                            "Cidade": "Olinda",
                            "UF": "BA",
                        }
                    },
                    {
                        "Nome": "RacionaisMC",
                        "CPF": "12345678905",
                        "DataDeNascimento": date(year=1994, month=9, day=24),
                        "Sexo": "Masculino",
                        "Contatos": 
                        {
                            "Email":"dfgdg@asdd.com",
                            "Tel01": 27987654321,
                            "Tel02": 28123456789
                        },
                        "Endereço":
                        {
                            "CEP": 29145250,
                            "Num": 151,
                            "Logradouro": "Beco 10",
                            "Bairro": "Capão Redondo",
                            "Cidade": "Cidade De Deus",
                            "UF": "SP",
                        }
                    }
                ]         
        """
        
        #subindo apenas um dado.
        if type(dados) == dict:
            
            resposta = self.collection.insert_one(dados)
            
            #Retornando o id do dado inserido.
            self.resposta = resposta.inserted_id            
        
        #subindo vários dados.    
        if type(dados) == list:
            
            resposta = self.collection.insert_many(dados)
            
            #Retornando os ids dos dados inseridos.
            self.resposta = resposta.inserted_ids 
    
    def consultarDados(self, filtro=None):
        """_summary_
         
         Função criada com a finalidade de consultar dados de uma Collection existentes. Para usa-la é necessário passar como parâmetro os seguintes itens:
            
            1ª) Inserir um dicionário com os parâmetros para que os dados sejam filtrados. Veja o exemplo abaixo:
            
                dictfiltro = 
                {
                    "qty":"quantidade máxima de dados que devem ser retornados",
                    "Nome do campo procurado": "Dado a ser buscado"
                }
            
            2ª) Para printar a resposta, é necessário do seguinte parâmetro:
            
                from pprint import pprint
                for dado in self.resposta:
                    pprint(dado)
                
            OBS: Caso o Filtro não seja declarado, a função irá retornar todos os dados da collection.
            
            
        """
        
        #   Buscando e retornando o primeiro dado da Collection.
        self.resposta  = self.collection.find_one()
        
        #   Buscando e retornando os dados encontrados na Collection.
        if filtro:
            self.resposta = self.collection.find(filtro)
