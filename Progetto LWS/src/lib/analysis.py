import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json

#  Salva plot risultante dall' analisi e stampa messaggio di log
def save_plot(output_directory, output_name):
    
    output_path = output_directory + output_name
    plt.savefig(output_path)
    print(f"[Crafty][Analysis] Risultato salvato in {output_path}")

# Esegue il plotting della distribuzione del numero di transazioni per blocco (occupazione del blocco), nell’intero periodo temporale considerato usando sia scala lineare sia logaritmica
def plot_tx_distribution_by_block(df_transactions, output_directory):
    
    # Raggrupo le transazioni per block id e conto le righe contenute da ogni gruppo (equivale a contate le tx per blocco), infine ottengo nuovo dataframe con reset_index()
    df_distribution = df_transactions.groupby("block_id").size().reset_index(name="transaction_count")

    plt.figure(figsize=(10, 4))

    # Plot istogramma utilizzando scala default 
    plt.subplot(1, 2, 1)
    plt.hist(df_distribution["transaction_count"], bins=50, edgecolor='black')
    plt.title("Distribution of Transactions per Block (Default Scale)")
    plt.xlabel("Number of Transactions per Block")
    plt.ylabel("Frequency")

    # Plot utilizzando scala logaritmica
    plt.subplot(1, 2, 2)
    plt.hist(df_distribution["transaction_count"], bins=50, edgecolor='black')
    plt.yscale('log')  # Setto asse y in scala logarimica per avere una migliore visualizzazione
    plt.title("Distribution of Transactions per Block (Log Scale)")
    plt.xlabel("Number of Transactions per Block")
    plt.ylabel("Frequency (Log)")

    save_plot(output_directory, "plot_tx_distribution_by_block.png")
    

# Esegue il plotting dell'evoluzione dell'occupazione dei blocchi nel tempo, considerando intervalli temporali di due mesi. In questo caso produce un grafico che riporta il numero di transazioni medie per ogni periodo considerato.
def plot_average_tx_by_block_2_month_period(df_transactions, output_directory):

    # converto i timestamp in datetime
    df_transactions['timestamp'] = pd.to_datetime(df_transactions['timestamp'], unit='s')

    # Raggrupo le transazioni per block id e ottengo due nuove colonne nel df risultante:
    #   - tx_count = numero di transazioni contenuto in quel blocco
    #   - timestamp = timestamp del blocco (dato che il timestamp è del blocco tutte le transazioni appartente allo stesso blocco hanno le stesso timestamp quindi prendo il primo)
    df_blocks = df_transactions.groupby('block_id').agg(
        tx_count=('tx_id', 'size'),
        timestamp=('timestamp', 'first')  
    ).reset_index()
    
    # Cambio il giorno della prima transazione dal 9-01-2009 al 01-01-2009 affinchè la Grouper definisca correttamente i periodi di 2 mesi (primo periodo che va dal 1 jan a 28 feb)
    df_blocks.loc[0, 'timestamp'] = df_blocks.loc[0, 'timestamp'].replace(day=1)
    
    # Raggrupo i blocchi per periodi di due mesi tramite il timestamp e ottengo la media di tutte le transazioni per blocco per ogni periodo ottenuto
    df_blocks  = df_blocks.groupby(pd.Grouper(key='timestamp', freq='2ME'))['tx_count'].mean().reset_index(name= "avg_tx_count") 
    
    plt.figure(figsize=(10, 4))
    plt.plot(df_blocks["timestamp"], df_blocks["avg_tx_count"], marker='o', linestyle='-')

    plt.xlabel('Date Period')
    plt.ylabel('Average Transactions per Block by Period')
    plt.title('Average Transactions per Block by 2-Month Period')
    plt.grid(True) # setto grid a True al fine di  migliorare la visualizzazione del plot
    plt.xticks(rotation=45) # ruoto label asse x a 45 gradi per evitare che i label si sovrappongano
    
    save_plot(output_directory, "plot_average_tx_by_block_2_month_period.png")
    

# Restituisce ammontare totale e numero degli UTXO al momento dell’ultima transazione registrata nella blockchain considerata 
def get_total_utxos(df_inputs, df_outputs, output_directory):  
    
    # Eseguo la left merge tra df_outputs e df_inputs confrontando la coppia ['tx_id', 'position'] output con la coppia ['prev_tx_id', 'prev_tx_pos'] associando cosi ad ogni output l'input dal quuale viene speso, NaN altrimenti
    utxo_df = pd.merge(df_outputs, df_inputs, left_on=['tx_id', 'position'], right_on=['prev_tx_id', 'prev_tx_pos'], how='left', indicator=True)
    # Filtro ora solo per gli output che non hanno corrispondeza negli input (prendendo solo le righe con indicator "left_only") ottenendo cosi tutti le UTXO
    utxo_df = utxo_df[utxo_df['_merge'] == 'left_only'].drop('_merge', axis=1)
    # Ottengo ammontare totale utxo facendo la somma di tutti gli amount e converto satoshi in btc (1 btc = 100000000 satoshi)
    total_amount_utxo_btc = (utxo_df['amount'].sum())/100000000
    # Ottengo anche il numero totale di utxo
    utxo_count = len(utxo_df)
    
    # Salvo risultati in un dizionario
    json_data = {}
    
    json_data["total_amount_utxo_btc"] = total_amount_utxo_btc
    
    json_data["utxo_count"] = utxo_count
    
    output_path = output_directory + "utxo.json"
    
    # Salvo json risultati
    with open(output_path, 'w') as json_file:
        json.dump(json_data, json_file, indent= 4)

    print(f'[Crafty][Analysis] Risultato salvato in {output_path}')


# Esegue il plotting della distribuzione degli intervalli di tempo che intercorrono tra la transazione che genera un valore in output (UTXO) e quella che lo consuma, per gli output spesi nel periodo considerato
def plot_utxo_distribution_by_time_to_spent(df_inputs, df_outputs, df_transactions, output_directory):     
    
    # Eseguo la inner merge tra df_outputs e df_inputs ottenendo solo gli output spesi (stxo) (in quanto inner == intersezione e il controllo restituisce solo gli output la cui coppia ['tx_id', 'position'] risulta presente in input ['prev_tx_id', 'prev_tx_pos']
    df_stxo = pd.merge(df_outputs, df_inputs, left_on=['tx_id', 'position'], right_on=['prev_tx_id', 'prev_tx_pos']).reset_index()
    # Rinomino le colonne del df ottentuto
    df_stxo = df_stxo.rename({"tx_id_x": "output_tx_id", "tx_id_y": "input_tx_id"}, axis= "columns")
    # Filtro lasciando solo le colonne contenenti output_tx_id (risalire alla generazione) e input_tx_id (risalire alla spesa)
    df_stxo = df_stxo[["output_tx_id", "input_tx_id"]]
    # Ottengo un df_transaction ridotto con le uniche colonne di cui necessito ovvero tx_id per la merge e timestamp per ottenere timestamp generazione utxo e timestamp spesa (stxo)
    reduced_df_transactions = df_transactions[["tx_id", "timestamp"]]
    # Eseguo un prima merge su "output_tx_id" (df_stxo) e "tx_id" (reduced_df_transactions) per ottenere i timestamp di generazione di ogni output
    df_stxo = pd.merge(df_stxo, reduced_df_transactions, left_on=['output_tx_id'], right_on=['tx_id']).rename({"timestamp": "timestamp_creation"}, axis= "columns").drop("tx_id", axis=1)
    # Eseguo la seconda merge su "input_tx_id" (df_stxo) e "tx_id" (reduced_df_transactions) per ottenere invece i timestamp di spesa di ogni output
    df_stxo = pd.merge(df_stxo, reduced_df_transactions, left_on=['input_tx_id'], right_on=['tx_id']).rename({"timestamp": "timestamp_spent"}, axis= "columns").drop("tx_id", axis=1)
    # Converto timestamp generazione e spesa a datetime
    df_stxo['timestamp_creation'] = pd.to_datetime(df_stxo['timestamp_creation'], unit='s')
    df_stxo['timestamp_spent'] = pd.to_datetime(df_stxo['timestamp_spent'], unit='s')
    # Aggiungo un nuova colonna "delta_time_days" dove salvare la differenza (in giorni) tra timestamp generazione e timestamp spesa
    df_stxo["delta_time_days"] = (df_stxo["timestamp_spent"] - df_stxo["timestamp_creation"]).dt.days
    # Plot istogramma utilizzando scala logaritmica
    plt.figure(figsize=(10, 4))
    
    plt.hist(df_stxo["delta_time_days"], bins=50, edgecolor='black')
    plt.title("Distribution of Delta-Time Spent Output")
    plt.xlabel("Days delta_time")
    plt.ylabel("Frequency")
    plt.yscale('log')  # Setto asse y in scala logarimica al fine di ottenere una migliore visualizzazione
    
    save_plot(output_directory, "plot_utxo_distribution_by_time_to_spent.png")
    
def get_correlation_df(df_inputs, df_outputs, df_transactions):   
    # Eseguo la merge tra df_inputs e df_outputs sulle coppie ("prev_tx_id", "prev_tx_pos") e ("tx_id", "position") per estendere ogni input con l'output speso da esso
    input_df = pd.merge(df_inputs, df_outputs, left_on=['prev_tx_id', 'prev_tx_pos'], right_on=['tx_id', 'position'])
    # Filtro il df ottenuto dalla merge prendendo solo le colonne "tx_id_x" e "amount" e le rinomino
    input_df = input_df[["tx_id_x", "amount"]].rename({"tx_id_x": "tx_id", "amount": "input_amount"}, axis= "columns")
    # Raggruppo gli input per "tx_id" ottenendo un df con un riga per ogni transazione con il corrispettivo amount totale, nominando la colonna ottenuta "tx_input_amount"
    input_df = input_df.groupby("tx_id")["input_amount"].sum().reset_index(name = "tx_input_amount")
    # Filtro il df delle transazione prendendo solo le colonne di cui necessito per l'analisi: "tx_id", "fee", "timestamp"
    reduced_df_transactions = df_transactions[["tx_id", "fee", "timestamp"]]
    # Eseguo la merge tra input_df e reduced_df_transactions su "tx_id" per estendere le transazioni con la loro fee e il loro timestamp, riordinando le righe ottenuto per timestamp (crescente)
    input_df = pd.merge(input_df, reduced_df_transactions, on= "tx_id").sort_values("timestamp")
    # Converto i timestamp in datetime
    input_df['timestamp'] = pd.to_datetime(input_df['timestamp'], unit='s')
    input_df.rename(columns={'timestamp': 'date'}, inplace=True)
    # Calcolo la fee in percentuale rispetto all amount speso per ogni transazione
    input_df["fee_percentage"] = (input_df["fee"] / input_df["tx_input_amount"]) * 100
    # Raggruppo le transazioni per periodi temporali di un giorno tramite la grouper, calcolando per ogni gruppo ottenuto: media delle fee, medie della percentuale delle fee e numero di transazioni
    input_df = input_df.groupby(pd.Grouper(key='date', freq='D')).agg(
        avg_fee=('fee', 'mean'),
        avg_fee_percentage=('fee_percentage', 'mean'), 
        tx_count=('tx_id', 'size'), 
    ).reset_index()
    
    # Rimuovo giorni in cui non vi sono state transazioni 
    input_df = input_df[input_df["tx_count"] > 0]
    
    return input_df

# Plotta due grafici sovrapposti per visualizzare correlazione tra avg_fee e tx_count (chain congestion, ovvero numero di transazioni) in frame temporali di un giorno. 
def plot_correliaton_between_avg_fees_and_tx_count(correlation_df, output_directory):
    
    # Plotting grafico tx_count in frame temporali giornalieri
    fig, ax1 = plt.subplots(figsize=(10, 4))
    ax1.plot(correlation_df["date"], correlation_df["tx_count"], '#142D4C', label='Chain Congestion')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Transactions', color='#142D4C')
    ax1.tick_params(axis='y', labelcolor='#142D4C')
    ax1.set_title('Comparison between chain congestion and avg fee in daily frame')

    # Plotting grafico sovrapposto (condividono asse x ma usano assi y differenti) per avg_fees in frame temporali giornalieri
    ax2 = ax1.twinx()
    ax2.plot(correlation_df["date"], correlation_df["avg_fee"], '#F7931B', label='Avg Fee by Date')
    ax2.set_ylabel('Avg Fee', color='#F7931B')
    ax2.tick_params(axis='y', labelcolor='#F7931B')
    ax2.legend(loc='upper right')
    
    save_plot(output_directory, "plot_correliaton_between_avg_fees_and_tx_count")

# Plotta due grafici sovrapposti per visualizzare correlazione tra avg_fee_percentage e tx_count (chain congestion, ovvero numero di transazioni) in frame temporali di un giorno. 
def plot_correliaton_between_avg_fees_percentage_and_tx_count(correlation_df, output_directory):
    
    # Plotting grafico tx_count in frame temporali giornalieri
    fig, ax1 = plt.subplots(figsize=(10, 4))
    ax1.plot(correlation_df["date"], correlation_df["tx_count"], '#142D4C', label='Chain Congestion')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Transactions', color='#142D4C')
    ax1.tick_params(axis='y', labelcolor='#142D4C')
    ax1.set_title('Comparison between chain congestion and avg fee percentage in daily frame')

    # Plotting grafico sovrapposto (condividono asse x ma usano assi y differenti) per avg_fees_percentage in frame temporali giornalieri
    ax2 = ax1.twinx()
    ax2.plot(correlation_df["date"], correlation_df["avg_fee_percentage"], '#F7931B', label='Avg Fee Percentage by Date')
    ax2.set_ylabel('Avg Fee Percentage', color='#F7931B')
    ax2.tick_params(axis='y', labelcolor='#F7931B')
    ax2.legend(loc='upper right')
    
    save_plot(output_directory, "plot_correliaton_between_avg_fees_percentage_and_tx_count")


# Dato un df e relative colonne e plotta la matrice di correlazione rispetto quello colonne
def plot_correlation_matrix(correlation_df, output_directory):
    
    plt.figure(figsize=(5, 4))
    # Ottengo matrice correlazione relativa a correlation_df
    correlation_matrix = correlation_df.corr()
    # Plotting matrice correlaione realtiva a correlation_df usando seaborn
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f")
    plt.title("Chain Congestion, Avg Fee Percentage e Avg Fee")

    save_plot(output_directory, "plot_correlation_matrix")
    
    
