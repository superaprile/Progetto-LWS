import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from statistics import mean, median, stdev
import json

def compute_clusters_analysis(clusters, output_directory):
    
    cluster_analysis = {}
    
    # Salvo numero di cluster ottenuti nel dictionary
    cluster_analysis["Numero totale cluster"] = len(clusters)
    
    # Calcolo lunghezza di ogni cluster e li salvo in una lista
    clusters_size = [len(cluster) for cluster in clusters]
    
    # Salvo dimensione minima cluster nel dictionary
    cluster_analysis["Dimensione minima cluster"] = min(clusters_size) 
    
    # Salvo dimensione massima cluster nel dictionary
    cluster_analysis["Dimensione massima cluster"] = max(clusters_size)
    
    # Salvo dimensione media cluster nel dictionary
    cluster_analysis["Dimensione media cluster"] = mean(clusters_size)
    
    # Salvo mediana cluster nel dictionary
    cluster_analysis["Mediana dimensione cluster"] = median(clusters_size)  
    
    # Salvo deviazione standard cluster nel dictionary, al fine di vedere quanto i valori divergono dalla media
    cluster_analysis["Deviazione standard dimensione cluster"] = stdev(clusters_size)
    
    with open(output_directory + "cluster_analysis.json", 'w') as json_file:
        json.dump(cluster_analysis, json_file, indent= 4)

    print(f"[Crafty][Cluster] Analisi cluster salvate in {output_directory + 'cluster_analysis.json'}")
    
    plt.figure(figsize=(10, 4))
    # Eseguo plotting istogramma distribuzione della dimensione dei cluster
    plt.hist(clusters_size, bins=100, alpha=0.7, color='blue', edgecolor='black')
    plt.xlabel('Dimensione del Cluster')
    plt.ylabel('Frequenza')
    plt.yscale('log')
    plt.title('Distribuzione delle dimensioni dei cluster')

    plt.savefig((output_directory + "cluster_size_distribution.png"))
    print(f"[Crafty][Cluster] Distribuzione dimensione dei cluster salvata in {output_directory + 'cluster_size_distribution.png'}")


# Resituisce cluster indirizzi, che equivalgono alle componenti connesse del grafo indiritto semplici avente come nodi gli indirizzi e archi creati seguendo common input heuristic
def get_address_clusters(df_inputs, df_outputs, df_mapping, output_directory):
    
    # Creo grafo semplice indiretto
    address_graph = nx.Graph()
    
    # Aggiungo un nodo al grafico per ogni address presente in df_mapping
    address_graph.add_nodes_from(df_mapping["hash"])
    
    # Eseguo inner merge tra df_inputs e df_outputs per estendere ogni input con corrsipettivo output speso da esso
    input_df = pd.merge(df_inputs, df_outputs, left_on=['prev_tx_id', 'prev_tx_pos'], right_on=['tx_id', 'position'])
    
    # Filtro colonne tenendo solo: tx_id_x in e address_id e rinomino tx_id_x in tx_id
    input_df = input_df[["tx_id_x", "address_id"]].rename({"tx_id_x": "tx_id"}, axis= "columns")
    
    # Eseguo inner merge tra input_df e df_mapping in modo da ottenere per ogni input il relativo address tramite la chiave address_id
    input_df = pd.merge(input_df, df_mapping, on= "address_id")[["tx_id", "hash"]]
    
    # Raggrupo gli input per tx_id, in modo da ottenere per ogni transazione il corrispetivo gruoppo di input
    input_grouped_by_tx = input_df.groupby("tx_id")
    
    # Itero i gruppi risultanti da input_df.groupby("tx_id"), ovvero le transazioni
    for _, group in input_grouped_by_tx:
        
        # Controllo che la transazione abbia più di input
        if len(group) > 1 :
            # Salvo address relativi a input della transazione in un set() in modo da rimuovere duplicati e prevenire formazione di self_loop sul grafo
            addresses = set(group["hash"])
            
            # Rimuovo dal set() ottenuto il primo indirizzo relativo al primo input e lo salvo in "first_address"
            first_address = addresses.pop()
            
            # Itero gli address relativi ai restanti input e per ognuno creo un arco da first_address ad esso
            for address in addresses:
                address_graph.add_edge(first_address, address)    
    
    # Calcolo le componenti connesse del grafo, le quali corrispondo ai cluster di address risultanti dall' applicazione della "common input heuristic". Infine li salvo in una lista per poterli iterare in seguito
    clusters = list(nx.connected_components(address_graph))
    
    # Ottengo dizionario con chiave "Cluster index" e valore lista indirizzi presenti nel cluster
    clusters_dict = {f'Cluster {i}': list(cluster) for i, cluster in enumerate(clusters, 1)}
    
    # Salvo cluster ottenuti
    with open(output_directory + "cluster.json", 'w') as json_file:
        json.dump(clusters_dict, json_file, indent= 4) 
        
    print(f"[Crafty][Cluster] Cluster indirizzi salvati in {output_directory + 'cluster.json'}")
    
    return clusters


