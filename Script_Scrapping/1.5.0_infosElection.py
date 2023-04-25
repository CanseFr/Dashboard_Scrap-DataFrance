import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv
import pprint
import os 
import json
from multiprocessing import Pool
import time
from pprint import pprint
import re


#__________________________________________________Brouillon__________
#_____________________________________________
# # Recherche Clé & valeur : candidats
# divs = soup.findAll('table', class_="od_table--grid--col3 elections_table--candidats-with-pic")
# # print(len(divs))
# # print(divs[0].text)
# tableau = divs[0]
# candidats = tableau.findAll('tr', class_=re.compile('color'))
# # print(len(candidats))
# for candida in candidats:
#     # print(candida.find('strong').text)# nomcandidats
#     cle = candida.find('strong').text 
#     # print(candida.find('span').text)  # parti
#     valeur = candida.findAll('td')[1].text.replace(',','.').replace('%','')
#     dico[cle] = valeur    # affectation dico 
#     # pprint(dico)        #Apercu
#     # print(dico.keys())
#     # print(candida.findAll('td')[1].text.replace(',','.').replace('%',''))

# # _____________________________________________

# # Recherche Clé & valeur : participation scrutin
# table = soup.findAll('table', class_="od_table--grid--col2")

# mesTr = table[0]

# #Clé 
# for i in range(0,10,2):
#     # print(mesTr.findAll('td')[i].text)
#     cle = mesTr.findAll('td')[i].text
# # Valeur
#     for i in range(1,10,2):
#         # print(mesTr.findAll('td')[i].text.replace('%',''))    
#         valeur = mesTr.findAll('td')[i].text.replace(',','.').replace('%','').replace(' ','')
#         # print(valeur.text.replace(',','.').text.replace('%','').text.replace(' ',''))
#         try :
#             dico[cle] = float(valeur)
#         except :
#             dico[cle] = valeur
            
# # pprint(dico)
#__________________________________________________Brouillon__________
#__________________________________________________



#___________________________________________________________________________________
#___________________________________________________________________________________
#_________________________PROPRE ___________________________________________________
#___________________________________________________________________________________
#___________________________________________________________________________________

colonnes = ['ville','lien','Nathalie LOISEAU', 'Yannick JADOT', 'François-Xavier BELLAMY', 'Raphaël GLUCKSMANN', 
            'Jordan BARDELLA', 'Manon AUBRY', 'Benoît HAMON', 'Ian BROSSAT', 'Jean-Christophe LAGARDE',
            'Dominique BOURG', 'Hélène THOUY', 'Nicolas DUPONT-AIGNAN', 'François ASSELINEAU', 
            'Florie MARIE', 'Nathalie ARTHAUD', 'Florian PHILIPPOT', 'Francis LALANNE', 'Nagib AZERGUI', 
            'Sophie CAILLAUD', 'Nathalie TOMASINI', 'Olivier BIDOU', 'Yves GERNIGON', 'Pierre DIEUMEGARD',
            'Christian Luc PERSON', 'Thérèse DELFEL', 'Audric ALEXANDRE', 'Hamada TRAORÉ',
            'Robert DE PREVOISIN', 'Vincent VAUCLIN', 'Gilles HELGEN', 'Antonio SANCHEZ', 
            'Renaud CAMUS', 'Christophe CHALENÇON', 'Cathy Denise Ginette CORBET','Taux de participation'
            ,'Taux d\'abstention','Votes blancs (en pourcentage des votes exprimés)','Votes nuls (en pourcentage des votes exprimés)',
            'Nombre de votants']

#~___________Ici on peut copier la fonction qui fait la difference, bienc hanger les nom de fichier !!!
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

if os.path.isfile('dataset\\election.csv'): # Nomemr le nom du nouveau fichier 
    tableauElection = pd.read_csv('dataset\\election.csv',error_bad_lines=False) # ,error_bad_lines=False, dtype='unicode' // Si "error tokenizing" 
    colonnes1 = tableauElection['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv') 
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2) 
else :
    tableauElection = DataFrame(columns=colonnes)
    tableauElection.to_csv('dataset\\election.csv' , index=False) # Ici aussi 
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']
#~___________

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']  # Gestion erreur 

def parse(lien):
    dico = {i : "" for i in colonnes}
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    #######Ici on va prendre le lien de notre pont,mais seulement a partir du 18eme index
    req = requests.get("https://election-europeenne.linternaute.com/resultats/" + lien[18:])
    #######  index :                                     012..............18                         
    #     url pont :                                     /management/ville/saint-genest/ville-03233
    # url officiel : https://election-europeenne.linternaute.com/resultats/saint-genest/ville-03233
    # url requete  : https://election-europeenne.linternaute.com/resultats + pont
    #######
    time.sleep(2)

    if req.status_code == 200:
        with open('dataset\\election.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            divs = soup.findAll('table', class_="od_table--grid--col3 elections_table--candidats-with-pic")

            tableau = divs[0]
            candidats = tableau.findAll('tr', class_=re.compile('color'))

            for candida in candidats:
                cle = candida.find('strong').text 
                valeur = candida.findAll('td')[1].text.replace(',','.').replace('%','')
                try :
                    dico[cle] = float(valeur)   
                except : 
                    dico[cle] = valeur     

            table = soup.findAll('table')

            if len(table) == 2:
                for info in table[1].findAll('tr')[1:]:
                    cle = info.findAll('td')[0].text
                    valeur = info.findAll('td')[1].text.replace(',','.').replace('%','').replace(' ','')
                    try :
                        dico[cle] = float(valeur)
                    except :
                        dico[cle] = valeur



            writer.writerow(dico)
            print("[ Election ] :", lien)
            
            
if __name__=="__name__":
    with Pool(20) as p:
        p.map(parse,listeLiens)

            
            