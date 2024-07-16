from .utils import *

def get_wallet_from_address_bic(address):
    
    # Definisco headers per la richiesta GET contenente user-agent scelto randomicamente
    headers = {"User-Agent": get_random_user_agent()}  
    
    # Eseguo richiesta GET a https://bitinfocharts.com/bitcoin/address/address_da_controllare per ottenere il corrispettivo html
    resp = requests.get("https://bitinfocharts.com/bitcoin/address/" + address, headers=headers)
    
    # Controllo eventuale errore richesta tramite status code risposta
    if resp.status_code != 200:
        
        # Richiesta non andata a buon fine, aspetto 10 secondi e riprovo
        get_wallet_from_address_bic(address)
    
    else:

        # Richiesta andata a buon fine, instanzio parser html beautifoulsoup e parso wallet contenuto in soup.find('a', style='color: #018174').text.replace("wallet: ", "")
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        wallet = soup.find('a', style='color: #018174').text.replace("wallet: ", "")
        
        return wallet
    
    return None
       
def deanomize_cluster_bic(cluster_name, cluster, max_addresses_to_check):

    total_addresses = min(max_addresses_to_check, len(cluster)) if max_addresses_to_check != -1 else len(cluster)
    
    
    with tqdm(total=total_addresses, desc=f"[Crafty][Deanonymization] Deanonymizing {cluster_name} through bitcoininfocharts") as pbar:
        # Itero primi 20 address del cluster
        for address in cluster[:max_addresses_to_check] if max_addresses_to_check != -1 else cluster:
            # Ottengo wallet relativo all'address
            wallet = get_wallet_from_address_bic(address)
            
            # Controllo wallet != None e che non inizi sia numerico affinchè sia associato ad un servizio
            if wallet and (not wallet.isdigit()):

                # Restituisco coppia wallet e indirizzo associato
                return wallet, address
            
            # Sleep random tra ogni richiesta nell'intervallo (0,1) secondi
            pbar.update(1)
            time.sleep(random.uniform(0, 1)) 
        
    return None