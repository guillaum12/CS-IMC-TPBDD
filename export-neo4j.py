import os
import dotenv
import pyodbc
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node

dotenv.load_dotenv(override=True)

server = os.environ["TPBDD_SERVER"]
database = os.environ["TPBDD_DB"]
username = os.environ["TPBDD_USERNAME"]
password = os.environ["TPBDD_PASSWORD"]
driver= os.environ["ODBC_DRIVER"]

neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
neo4j_user = os.environ["TPBDD_NEO4J_USER"]
neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))

BATCH_SIZE = 10000

print("Deleting existing nodes and relationships...")
graph.run("MATCH ()-[r]->() DELETE r")
graph.run("MATCH (n:Artist) DETACH DELETE n")
graph.run("MATCH (n:Film) DETACH DELETE n")

with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
    cursor = conn.cursor()

    # --- SECTION 1 : FILMS ---
    exportedCount = 0
    cursor.execute("SELECT COUNT(1) FROM TFilm")
    totalCount = cursor.fetchval()
    cursor.execute("SELECT idFilm, primaryTitle, startYear FROM TFilm")
    while True:
        importData = []
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            # Création du nœud avec les propriétés récupérées de SQL Server
            n = Node("Film", idFilm=row[0], primaryTitle=row[1], startYear=row[2])
            importData.append(n)

        try:
            create_nodes(graph.auto(), importData, labels={"Film"})
            exportedCount += len(rows)
            print(f"{exportedCount}/{totalCount} title records exported to Neo4j")
        except Exception as error:
            print(error)

    # --- SECTION 2 : ARTISTES (Names) ---
    exportedCount = 0
    cursor.execute("SELECT COUNT(1) FROM tArtist")
    totalCount = cursor.fetchval()
    cursor.execute("SELECT idArtist, primaryName, birthYear FROM tArtist")
    while True:
        importData = []
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            # Création du nœud Artist
            n = Node("Artist", idArtist=row[0], primaryName=row[1], birthYear=row[2])
            importData.append(n)

        try:
            create_nodes(graph.auto(), importData, labels={"Artist"})
            exportedCount += len(rows)
            print(f"{exportedCount}/{totalCount} artist records exported to Neo4j")
        except Exception as error:
            print(error)

    # --- INDEXATION ---
    # try:
    #     print("Indexing Film nodes...")
    #     # Updated syntax: CREATE INDEX [optional_name] FOR (n:Label) ON (n.property)
    #     graph.run("CREATE INDEX film_id_index FOR (f:Film) ON (f.idFilm)")

    #     print("Indexing Name (Artist) nodes...")
    #     graph.run("CREATE INDEX artist_id_index FOR (a:Artist) ON (a.idArtist)")
        
    # except Exception as error:
    #     print(f"Error: {error}")

    # --- SECTION 3 : RELATIONS (tJob) ---
    exportedCount = 0
    cursor.execute("SELECT COUNT(1) FROM tJob")
    totalCount = cursor.fetchval()
    cursor.execute(f"SELECT idArtist, category, idFilm FROM tJob")
    while True:
        # Dictionnaire pour grouper les relations par type avant l'import en vrac
        importData = { "acted in": [], "directed": [], "produced": [], "composed": [] }
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            # Structure du tuple pour py2neo.bulk: (start_node_key, properties, end_node_key)
            relTuple = (row[0], {}, row[2])
            if row[1] in importData:
                importData[row[1]].append(relTuple)

        try:
            for cat in importData:
                if importData[cat]: # On n'importe que s'il y a des données
                    # Remplacement des espaces par des underscores et mise en majuscule (convention Neo4j)
                    rel_type = cat.replace(" ", "_").capitalize()
                    
                    # Utilisation de create_relationships pour lier les nœuds existants via leurs IDs
                    create_relationships(graph.auto(), importData[cat], rel_type, 
                                         start_node_key=("Artist", "idArtist"), 
                                         end_node_key=("Film", "idFilm"))
            
            exportedCount += len(rows)
            print(f"{exportedCount}/{totalCount} relationships exported to Neo4j")
        except Exception as error:
            print(error)

print("Export terminé avec succès.")