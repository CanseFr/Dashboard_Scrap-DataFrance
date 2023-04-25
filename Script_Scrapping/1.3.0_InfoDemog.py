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
import pprint

# # @ Le defi ici est d'iterrer a traver les tableau pour recuperer nos clé et valeur 
# #   Le deuxieme defis et d'uniformiser les CLÉ { avoir des clés unique} ainsi que les VALEURS (avoir que des int ou des float)
# #   quand on etudie les données de l page web on peut constater qu'il y a beaucoup de valeur numerique ce qui est un plus pour le netoyage des données

# #*-*Pour travailler sur des valeur int il faut les debarasser des informations qui l'accompagne
# #les valeur a virgule sont ecrit avec ',' et non "." il faut donc fair un replace pour pouvoir travailelr dessus plus tard



# # I Recuperation des clé et valeur et traitement


# colonnes = ["ville","lien","Population" ,"Densité de population" ,"Nombre de ménages" ,"Habitants par ménage" ,"Nombre de familles" ,"Naissances" ,"Décès" ,"Solde naturel" ,"Hommes" ,"Femmes" ,"Moins de 15 ans" ,"15 - 29 ans" ,"30 - 44 ans" ,"45 - 59 ans" ,"60 - 74 ans" ,"75 ans et plus" ,"Agriculteurs exploitants" ,"Artisans, commerçants, chefs d'entreprise" ,"Cadres et professions intellectuelles supérieures" ,"Professions intermédiaires" ,"Employés" ,"Ouvriers" ,"Familles monoparentales" ,"Couples sans enfant" ,"Couples avec enfant" ,"Familles sans enfant" ,"Familles avec un enfant" ,"Familles avec deux enfants" ,"Familles avec trois enfants" ,"Familles avec quatre enfants ou plus" ,"Personnes célibataires" ,"Personnes mariées" ,"Personnes divorcées" ,"Personnes veuves" ,"Personnes en concubinage" ,"Personnes pacsées" ,"Population étrangère" ,"Hommes étrangers" ,"Femmes étrangères" ,"Moins de 15 ans étrangers" ,"15-24 ans étrangers" ,"25-54 ans étrangers" ,"55 ans et plus étrangers" ,"Population immigrée" ,"Hommes immigrés" ,"Femmes immigrées" ,"Moins de 15 ans immigrés" ,"15-24 ans immigrés" ,"25-54 ans immigrés" ,"55 ans et plus immigrés"]

# lien = "/management/ville/paris/ville-75056/"

# req = requests.get("https://www.journaldunet.com" + lien + "/demographie") # Pour le pont de notre fichier il faut rajouter l'extension demographie a l'url
# contenu = req.content
# soup = bs(contenu,"html.parser")

# # Recuperation des tableaux 
# tables = soup.findAll('table', class_="odTable odTableAuto")

# for i in range(len(tables)):
#     infos = tables[i].findAll('tr')
#     for info in infos[1:]:
#         cle = info.findAll('td')[0].text
#         valeur = info.findAll('td')[1].text
#         print(cle.split('(')[0].strip()," : ", float(valeur.split('h')[0].strip().replace(',','.'))) # *-*
#         # print(cle.split('(')[0].strip()) # cette ligne me sert a generer les clé de mon dictionnaire
             

# # A Recuperer des données de type script au format JSON, JSON fournit des dictionnaire !

# # 1 Recuperation des graphique 

# divs = soup.findAll('div', class_="section-wrapper")
# for div in divs :
#     titre_h2 = div.find('h2')
    
#     # Nombre d'habitants
#     if titre_h2 != None and "Nombre d'habitants" in titre_h2.text : # None permet de voir si il existe bien 
#         js_script = div.find('script').string
#         json_data = json.loads(js_script)
#         # pprint(json_data)
#         # print(json_data['series'][0]['data'])
#         # print(json_data['xAxis']['categories'])
#         annees = json_data['xAxis']['categories']
#         habitants = json_data['series'][0]['data']
        
#         for annee,habitant in zip(annees,habitants):
#             print(annee," : ", habitant)
    
#     # Naissances et décès     // Ici il y a deux courbes a choper 
#     if titre_h2 != None and "Naissances et décès" in titre_h2.text :
#         js_script = div.find('script').string
#         json_data = json.loads(js_script)
#         annees = json_data['xAxis']['categories']
#         naissance = json_data['series'][0]['data']
#         deces = json_data['series'][1]['data']
        
#         for annee, n, d in zip(annees, naissance, deces):
#             print(annee, n, d)
    
#     # Nombre d'étrangers
#     if titre_h2 != None and "Nombre d'étrangers" in titre_h2.text :
#         js_script = div.find('script').string
#         json_data = json.loads(js_script)
#         annees = json_data['xAxis']['categories']
#         etrangers = json_data['series'][0]['data']
        
#         for annee,etranger in zip(annees,etrangers):
#             print(annee," : ", etranger)
    
#     # Nombre d'immigrés
#     if titre_h2 != None and "Nombre d'immigrés" in titre_h2.text :
#         js_script = div.find('script').string
#         json_data = json.loads(js_script)
#         annees = json_data['xAxis']['categories']
#         immigres = json_data['series'][0]['data']
        
#         for annee,immigre in zip(annees,immigres):
#             print(annee," : ", immigre)

# #_______________________________________________________________________________________________

# II Ecrire dans dictionnaire

listeCles = ["ville","lien","Population" ,"Densité de population" ,"Nombre de ménages" ,"Habitants par ménage" ,"Nombre de familles" ,"Naissances" ,"Décès" ,"Solde naturel" ,"Hommes" ,"Femmes" ,"Moins de 15 ans" ,"15 - 29 ans" ,"30 - 44 ans" ,"45 - 59 ans" ,"60 - 74 ans" ,"75 ans et plus" ,"Agriculteurs exploitants" ,"Artisans, commerçants, chefs d'entreprise" ,"Cadres et professions intellectuelles supérieures" ,"Professions intermédiaires" ,"Employés" ,"Ouvriers" ,"Familles monoparentales" ,"Couples sans enfant" ,"Couples avec enfant" ,"Familles sans enfant" ,"Familles avec un enfant" ,"Familles avec deux enfants" ,"Familles avec trois enfants" ,"Familles avec quatre enfants ou plus" ,"Personnes célibataires" ,"Personnes mariées" ,"Personnes divorcées" ,"Personnes veuves" ,"Personnes en concubinage" ,"Personnes pacsées" ,"Population étrangère" ,"Hommes étrangers" ,"Femmes étrangères" ,"Moins de 15 ans étrangers" ,"15-24 ans étrangers" ,"25-54 ans étrangers" ,"55 ans et plus étrangers" ,"Population immigrée" ,"Hommes immigrés" ,"Femmes immigrées" ,"Moins de 15 ans immigrés" ,"15-24 ans immigrés" ,"25-54 ans immigrés" ,"55 ans et plus immigrés"]
dico = {                                # fusion de dictionnaire, dicoS dans dico
    **{ i : "" for i in listeCles},
    **{ "nbre habitants (" + str(a) + ")" : '' for a in range(2006,2020)}, # apres plusieur erreur j'ai fini par rajouter +1 a chaque anné du 2eme arguments de range
    **{ "nbre naissances (" + str(a) + ")" : '' for a in range(1999,2021)},
    **{ "nbre deces (" + str(a) + ")" : '' for a in range(1999,2021)},
    **{ "nbre étrangers (" + str(a) + ")" : '' for a in range(2006,2020)},
    **{ "nbre immigrés (" + str(a) + ")" : '' for a in range(2006,2020)}
    } # Range des années recolté

colonnes = list(dico.keys()) # colonnes pour mon dataframe et  egale a une liste des clés de mon dictionnaire

# Ici on peut copier al fonction qui fait la difference

def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

if os.path.isfile('dataset\\demographie.csv'): # Nomemr le nom du nouveau fichier 
    tableaDemo = pd.read_csv('dataset\\demographie.csv',error_bad_lines=False) # ,error_bad_lines=False, dtype='unicode' // Si "error tokenizing" 
    colonnes1 = tableaDemo['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv') 
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2) 
else :
    tableaDemo = DataFrame(columns=colonnes)
    tableaDemo.to_csv('dataset\\demographie.csv' , index=False) # Ici aussi 
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if lien[:11] == '/management'] # Gestion erreur 

# Ici on copie notre fonction parse
def parse(lien):
    # on decalle l'initialisationd e notre dictionnaire ici
    dico = {                                # fusion de dictionnaire, dicoS dans dico
    **{ i : "" for i in listeCles},
    **{ "nbre habitants (" + str(a) + ")" : '' for a in range(2006,2020)},
    **{ "nbre naissances (" + str(a) + ")" : '' for a in range(1999,2021)},
    **{ "nbre deces (" + str(a) + ")" : '' for a in range(1999,2021)},
    **{ "nbre étrangers (" + str(a) + ")" : '' for a in range(2006,2020)},
    **{ "nbre immigrés (" + str(a) + ")" : '' for a in range(2006,2020)}
    }  
    
    # Affectation de donnée basic
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]


    req = requests.get("https://www.journaldunet.com" + lien + "/demographie")
    time.sleep(2)

    if req.status_code == 200 :
        with open('dataset\\demographie.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            # Recuperation des tableaux 
            tables = soup.findAll('table', class_="odTable odTableAuto")

            for i in range(len(tables)):
                infos = tables[i].findAll('tr')
                for info in infos[1:]:
                    cle = info.findAll('td')[0].text
                    valeur = info.findAll('td')[1].text
                    
                    # affectation des clé et valeur
                    cle = cle.split('(')[0].strip()
                    valeur = valeur.split('h')[0].strip().replace(',','.')
                    try :
                        dico[cle] = float(valeur) # Parfois on peut tomber sur un "NC" alors il est impossible de convertir cette valeur en float, d'ou le try except
                    except :
                        dico[cle] = valeur
                

            #  Recuperation des graphique 

            divs = soup.findAll('div', class_="section-wrapper")
            for div in divs :
                titre_h2 = div.find('h2')
                
                # Nombre d'habitants
                if titre_h2 != None and "Nombre d'habitants" in titre_h2.text : # etre sure du titre
                    if div.find('script').string :                              # etre sure de la presence du script pour eviter de planter le prog
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        habitants = json_data['series'][0]['data']
                        
                        for annee,habitant in zip(annees,habitants):
                            try :
                                dico["nbre habitants (" + str(annee) + ")"] = float(habitant)
                            except :
                                dico["nbre habitants (" + str(annee) + ")"] = ''
                
                # Naissances et décès     
                if titre_h2 != None and "Naissances et décès" in titre_h2.text :
                    if div.find('script').string :
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        if len(json_data['series']) != 0  : # si variable presente on recupere sinon non 
                            naissance = json_data['series'][0]['data']
                            deces = json_data['series'][1]['data']
                            
                            for annee, n, d in zip(annees, naissance, deces):
                                try :
                                    dico["nbre naissances (" + str(annee) + ")"] = float(n)
                                    dico["nbre deces (" + str(annee) + ")"] = float(d)
                                except :
                                    dico["nbre naissances (" + str(annee) + ")"] = ''
                                    dico["nbre deces (" + str(annee) + ")"] = ''
                        else :
                            dico["nbre naissances (" + str(annee) + ")"] = ''
                            dico["nbre deces (" + str(annee) + ")"] = ''
                        
                        
                # Nombre d'étrangers
                if titre_h2 != None and "Nombre d'étrangers" in titre_h2.text :
                    if div.find('script').string :
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        etrangers = json_data['series'][0]['data']
                        
                        for annee,etranger in zip(annees,etrangers):
                            try :
                                dico["nbre étrangers (" + str(annee) + ")"] = float(etranger)
                            except :
                                dico["nbre étrangers (" + str(annee) + ")"] = ''
                
                # Nombre d'immigrés
                if titre_h2 != None and "Nombre d'immigrés" in titre_h2.text :
                    if div.find('script').string :
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        immigres = json_data['series'][0]['data']
                        
                        for annee,immigre in zip(annees,immigres):
                            try :
                                dico["nbre immigrés (" + str(annee) + ")"] = float(immigre)
                            except :
                                dico["nbre immigrés (" + str(annee) + ")"] = ''  
            writer.writerow(dico)
            print("[Demographie] : ", lien )
            
if __name__ == "__main__":
    with Pool(20) as p :
        p.map(parse, listeLiens)
                        