from .utils import *

def get_wallet_from_address_we(address):
    
    # Definisco headers per la richiesta GET contenente user-agent scelto randomicamente
    headers = {"User-Agent": get_random_user_agent()}  
    
    # Eseguo richiesta GET a https://www.walletexplorer.com/address/address_da_controllare per ottenere il corrispettivo html
    resp = requests.get("https://www.walletexplorer.com/address/" + address, headers=headers)
        
    # Controllo eventuale errore richesta tramite status code risposta
    if resp.status_code != 200:
        
        # Richiesta non andata a buon fine, aspetto 10 secondi e riprovo
        time.sleep(10)
        get_wallet_from_address_we(address)
    
    else:

        # Richiesta andata a buon fine, instazio parser html beautifoulsoup e parso wallet contenuto in (soup.find("div", class_="walletnote")).find("a").text
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        wallet = (soup.find("div", class_="walletnote")).find("a").text
        
        return wallet
    
    return None


def deanomize_cluster_we(cluster_name, cluster, max_addresses_to_check):
    
    total_addresses = min(max_addresses_to_check, len(cluster)) if max_addresses_to_check != -1 else len(cluster)
    
    
    with tqdm(total=total_addresses, desc=f"[Crafty][Deanonymization] Deanonymizing {cluster_name} through walletExplorer") as pbar:
    # Itero primi 20 address del cluster
        for address in cluster[:max_addresses_to_check] if max_addresses_to_check != -1 else cluster:
            
            # Ottengo wallet relativo all'address
            wallet = get_wallet_from_address_we(address)
            
            # Controllo wallet != None e che non inizi con "[" affinchè sia associato ad un servizio
            if wallet and (not wallet.startswith("[")):
                
                # Restituisco coppia wallet e indirizzo associato
                return wallet, address
            
            # Sleep random tra ogni richiesta nell'intervallo (1,2) secondi
            pbar.update(1)
            time.sleep(random.uniform(1, 2)) 
    
    return None