import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv
import pprint
import os # manipuler fichier 



# I Mise en place de fonction pour relancer un telechargement en cas d'interuption 
# Avec le tableau qui contient les liens URL que l'on doit scrapper, cette collones va nous servi
# de repair pour savoir ou on en est dans le telechargement et ensuite reprendre a cet endroit precis

# 1 deffinition pour fair une difference entre deux liste 
def diff(list1, list2):
    """Fair difference entre deux liste:
    - set : fonction qui retire les doublon 
    -  symmetric_diference :  fair une difference simetrique ligne par ligne
    
    Args:
        list1 (de repere, notre fichier qui contient les liens): _description_
        list2 (de telechargement en cours): _description_
    """
    return list(set(list1).symmetric_difference(set(list2)))

colonnes = ["ville" ,"lien" ,"Région" ,"Département" ,"Etablissement public de coopération intercommunale (EPCI)" ,
"Code postal (CP)" ,"Code Insee" ,"Nom des habitants" ,"Population (2019)" ,
"Population : rang national (2019)" ,"Densité de population (2019)" ,"Taux de chômage (2019)" ,"Pavillon bleu" ,
"Ville d'art et d'histoire" ,"Ville fleurie" ,"Ville internet" ,"Superficie (surface)" ,"Altitude min." ,
"Altitude max." ,"Latitude" ,"Longitude"]

# 2 Verifier si notre fichier existe deja ou non
if os.path.isfile('dataset\\infos.csv'): # Si le fichier existe cela veut dire qu'un session a deja eté effectué
    tableauInfos = pd.read_csv('dataset\\infos.csv') # donc on va recuperer le tableau des infos
    colonnes1 = tableauInfos['lien'] # et dans ce tableau la colonnes lien 
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv') # on recup le tableau des lien
    colonnes2 = tableauLiens['lien'] # on prend la colonne lien 
    listeLiens = diff(colonnes1, colonnes2) # on prend nos deux collones et on compare via la fonction crée plutot 
else : # dans le cas contraire si il n'existe pas il faut le creer 
    tableauInfos = DataFrame(columns=colonnes)
    tableauInfos.to_csv('dataset\\infos.csv' , index=False)
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

cpt = 0

# print(len(listeLiens)) # print de verification , commenter tous le bas pour affichier un difference


dico = {i : "" for i in colonnes}

with open('dataset\\infos.csv', 'a', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
    for lien in listeLiens :
        req = requests.get("https://www.journaldunet.com" + lien)
        contenu = req.content
        soup = bs(contenu, "html.parser")

        dico['lien'] = lien
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

        tables = soup.findAll('table', class_ = "odTable odTableAuto")

        for i in range(len(tables)):
            tousLesTr = tables[i].findAll('tr') 
             
            for tr in tousLesTr[1:]: 
                cle = tr.findAll('td')[0].text
                valeur = tr.findAll('td')[1].text
                
                if "Nom des habitants" in cle:
                    dico["Nom des habitants"] = valeur 
                elif "Taux de chômage" in cle : 
                    dico["Taux de chômage (2019)"] = valeur
                else : 
                    dico[cle] = valeur
    
        writer.writerow(dico)
        cpt += 1
        print(cpt,"/ 34 955")             
        print(lien)    






