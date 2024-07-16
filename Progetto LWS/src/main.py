import pandas as pd
from lib.analysis import *
from lib.cluster import *
from lib.scraper.bic_scraper import *
from lib.scraper.we_scraper import *
import json 
import os

def save_json(data, file_path):
    
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)

def compute_analysis(df_transactions, df_inputs, df_outputs):
    
    # Creo cartella dove salvare plot risultanti dalle analisi
    output_directory = "../output/analysis/"
    os.makedirs(output_directory, exist_ok=True)
    
    print("[Crafty][Analysis] Esecuzione 1° analisi in corso ...")
    # Eseguo la prima analisi
    plot_tx_distribution_by_block(df_transactions, output_directory)

    print("[Crafty][Analysis] Esecuzione 2° analisi in corso ...")
    # Eseguo la seconda analisi
    plot_average_tx_by_block_2_month_period(df_transactions, output_directory)

    print("[Crafty][Analysis] Esecuzione 3° analisi in corso ...")
    # Eseguo la terza analisi
    get_total_utxos(df_inputs, df_outputs, output_directory)

    print("[Crafty][Analysis] Esecuzione 4° analisi in corso ...")
    # Eseguo la quarta analisi
    plot_utxo_distribution_by_time_to_spent(df_inputs, df_outputs, df_transactions, output_directory)
    
    # Creo subfolder dove salvare plot risultanti da analisi a scelta
    output_directory = "../output/analysis/optional/"
    os.makedirs(output_directory, exist_ok=True)

    print("[Crafty][Analysis] Esecuzione analisi a scelta in corso ...")
    # Ottengo df utilizzato per il plotting dell' anlisi a scelta
    correlation_df = get_correlation_df(df_inputs, df_outputs, df_transactions)
    
    # Eseguo analisi a scelta
    plot_correliaton_between_avg_fees_and_tx_count(correlation_df, output_directory)
    plot_correliaton_between_avg_fees_percentage_and_tx_count(correlation_df, output_directory)
    plot_correlation_matrix(correlation_df, output_directory)
    
def get_and_analyze_clusters(df_inputs, df_outputs, df_mapping):
    
    # Creo cartella dove salvare plot risultanti dalle analisi
    output_directory = "../output/cluster/"
    os.makedirs(output_directory, exist_ok=True)
    
    # Genera grafo indirizzi e calcolo cluster (componenti connesse)
    print("[Crafty][Cluster] Creazione grafo e generazione cluster indirizzi in corso  ...")
    clusters = get_address_clusters(df_inputs, df_outputs, df_mapping, output_directory)
    
    # Effettua analisi richieste sui cluster
    print("[Crafty][Cluster] Esecuzione analisi cluster in corso ...")
    compute_clusters_analysis(clusters, output_directory)
    
    return clusters
        
def deanomize_top_10_clusters(clusters):
        
    # Creo cartella dove salvare risultati deanomizzazione
    output_directory = "../output/deanonymization/"
    os.makedirs(output_directory, exist_ok=True)
    
    # Eseguo il sorting in ordine decrescente dei cluster per dimensione
    sorted_clusters = sorted(clusters, key=len, reverse=True)
    
    # Ottengo i 10 cluster di dimensione maggiori
    top_10_clusters = sorted_clusters[:10]
    
    # Definisco i dictionaries dove salvare i cluster deanonimizzati
    we_deanomized_cluster = {}
    bic_deanomized_cluster = {}
    
    # Converto lista di cluster a dicitonary
    top_10_clusters = {f"Cluster {i}": list(cluster) for i, cluster in enumerate(top_10_clusters, 1)}
    
    # Setto path per salvare json contenente 10 cluster di dimensione maggiore
    output_path = output_directory + "top_10_cluster.json"
    
    # Salvo top 10 cluster
    save_json(top_10_clusters, output_path)
        
    print(f"[Crafty][Deanonymization] top 10 cluster salvati in {output_path}")
    
    max_addresses_to_check = int(input("[Crafty][Deanonymization] Inserire il massimo numero di indirizzi da provare per la deanonimizzazione di ciascun cluster, -1 per provarli tutti: "))
    
    # Itero i cluster tramite il dictionary
    for cluster_name, cluster_list in top_10_clusters.items():
        
        # Eseguo la deanonimizzazione mediante WalletExplorer
        result_we = deanomize_cluster_we(cluster_name, cluster_list, max_addresses_to_check)
        # Eseguo la deanonimizzazione mediante bitcoininfocharts
        result_bic = deanomize_cluster_bic(cluster_name, cluster_list, max_addresses_to_check)
        
        # Controllo se la deanonimizzazione tramite WalletExplorer ha avuto esito positivo
        if result_we:
            
            # Salvo indirizzo e wallet associato per il cluster deanonimizzato nel dictionary relativo WalletExplorer
            wallet, address = result_we
            we_deanomized_cluster[cluster_name] = (wallet, address)
        
        # Controllo se la deanonimizzazione tramite bitcoininfocharts ha avuto esito positivo    
        if result_bic:
            
            # Salvo indirizzo e wallet associato per il cluster deanonimizzato nel dictionary relativo bitcoininfocharts
            wallet, address = result_bic
            bic_deanomized_cluster[cluster_name] = (wallet, address)
    
    # Salvo risultati deanonimizzazione walletExplorer
    save_json(we_deanomized_cluster, output_directory + "walletExplorer.json")
    
    print(f"[Crafty][Deanonymization] Risultati deanonimizzazione walletExplorer salvati in {output_path}")
    
    # Salvo risultati deanonimizzazione bitcoininfocharts    
    save_json(bic_deanomized_cluster, output_directory + "bitcoininfocharts.json")
    
    print(f"[Crafty][Deanonymization] Risultati deanonimizzazione bitcoininfocharts salvati in {output_path}")
    
def main():
    
    # Leggo datasets transazioni, input e output
    print("[Crafty] Lettura datasets: transazioni, input, output in corso ...")
    df_transactions = pd.read_csv("../data/transactions.csv", names=["timestamp", "block_id", "tx_id", "is_coinbase", "fee"])
    df_inputs = pd.read_csv("../data/inputs.csv", names=["tx_id", "prev_tx_id", "prev_tx_pos"])
    df_outputs = pd.read_csv("../data/outputs.csv", names=["tx_id", "position", "address_id", "amount", "script_type"])
    print("[Crafty] Lettura avvenuta con successo")

    print("[Crafty] Inizio esecuzione analisi richieste dal progetto: ")
    # Eseguo analisi richieste nella prima parte del progetto
    compute_analysis(df_transactions, df_inputs, df_outputs)
    print("[Crafty] Analisi eseguite con successo")
    
    # Setto df_transactions a None in modo da eliminarne il riferimento, consentendone la deallocazione al garbage collector
    df_transactions = None
    
    # Leggo dataset mapping
    print("[Crafty] Lettura dataset df_mapping in corso ...")
    df_mapping = pd.read_csv("../data/mapAddr2Ids8708820.csv", names=["hash", "address_id"])
    print("[Crafty] Lettura avvenuta con successo")
    
    # Ottengo cluster indirizzi mediante uso di common input heuristic
    print("[Crafty] Inizio clusterizzazione indirizzi in corso  ...")
    clusters = get_and_analyze_clusters(df_inputs, df_outputs, df_mapping)
    print("[Crafty] Generazione cluster e relative analisi avvenute con successo")
    
    # Setto df_inputs, df_outputs, df_mapping a None in modo da eliminarne il riferimento, consentendone la deallocazione al garbage collector
    df_inputs, df_outputs, df_mapping = None, None, None
    
    
    # Effettuo deanonimizzazione dei 10 cluster di dimensione maggiore tramite walletExplorer e bitcoininfocharts
    print("[Crafty] Inizio deanonimizzazione indirizzi in corso ...")
    deanomize_top_10_clusters(clusters)
    print("[Crafty] Deanonimizzazione indirizzi avvenuta con successo")

    
if __name__ == "__main__":
    main()


