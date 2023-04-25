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
from pprint import pprint # Important de from et de import 

# # I Recuperation des clé et valeur de type santé et social 

# Liste clé OK
listeCles =  ["ville", "lien","Allocataires CAF" ,"Bénéficiaires du RSA" ,' - bénéficiaires du RSA majoré' ,' - bénéficiaires du RSA socle' ,"Bénéficiaires de l'aide au logement" ,
              " - bénéficiaires de l'APL (aide personnalisée au logement)" ," - bénéficiaires de l'ALF (allocation de logement à caractère familial)" ,
              " - bénéficiaires de l'ALS (allocation de logement à caractère social)" ," - bénéficiaires de l'Allocation pour une location immobilière" ,
              " - bénéficiaires de l'Allocation pour un achat immobilier" ,"Bénéficiaires des allocations familiales" ,' - bénéficiaires du complément familial' ,
              " - bénéficiaires de l'allocation de soutien familial" ," - bénéficiaires de l'allocation de rentrée scolaire" ,"Médecins généralistes" ,"Masseurs-kinésithérapeutes" ,
              "Dentistes" ,"Infirmiers" ,"Spécialistes ORL" ,"Ophtalmologistes" ,"Dermatologues" ,"Sage-femmes" ,"Pédiatres" ,"Gynécologues" ,"Pharmacies" ,"Urgences" ,"Ambulances" ,
              "Etablissements de santé de court séjour" ,"Etablissements de santé de moyen séjour" ,"Etablissements de santé de long séjour" ,"Etablissement d'accueil du jeune enfant" ,
              "Maisons de retraite" ,"Etablissements pour enfants handicapés" ,"Etablissements pour adultes handicapés" ,"Bénéficiaires de la PAJE" ,
              " - bénéficiaires de l'allocation de base" ," - bénéficiaires du complément mode de garde pour une assistante maternelle" ,
              " - bénéficiaires du complément de libre choix d'activité (CLCA ou COLCA)" ," - bénéficiaires de la prime naissance ou adoption"   ]



dico = {                                
    **{ i : "" for i in listeCles},
    **{ "nbre allocataires  (" + str(a) + ")" : '' for a in range(2009,2022)}, 
    **{ "nbre bénéficiaires RSA (" + str(a) + ")" : '' for a in range(2009,2022)},
    **{ "nbre APL (" + str(a) + ")" : '' for a in range(2009,2022)},
    **{ "nbre Alloc Familiales (" + str(a) + ")" : '' for a in range(2009,2022)}
    } # Range des années recolté

colonnes = list(dico.keys()) # colonnes pour mon dataframe et  egale a une liste des clés de mon dictionnaire


#~___________
# Ici on peut copier la fonction qui fait la difference, bienc hanger les nom de fichier !!!
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

if os.path.isfile('dataset\\santeSocial.csv'): # Nomemr le nom du nouveau fichier 
    tableauSante = pd.read_csv('dataset\\santeSocial.csv',error_bad_lines=False) # ,error_bad_lines=False, dtype='unicode' // Si "error tokenizing" 
    colonnes1 = tableauSante['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv') 
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2) 
else :
    tableauSante = DataFrame(columns=colonnes)
    tableauSante.to_csv('dataset\\santeSocial.csv' , index=False) # Ici aussi 
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management'] # Gestion erreur 
#~___________

# definition du parse :
def parse(lien):
    dico = {                                
        **{ i : "" for i in listeCles},
        **{ "nbre allocataires  (" + str(a) + ")" : '' for a in range(2009,2021)}, 
        **{ "nbre bénéficiaires RSA (" + str(a) + ")" : '' for a in range(2009,2021)},
        **{ "nbre APL (" + str(a) + ")" : '' for a in range(2009,2021)},
        **{ "nbre Alloc Familiales (" + str(a) + ")" : '' for a in range(2009,2021)}
        } 
    
    # Affectation de donnée fichier pont
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    # Ma requete
    req = requests.get("https://www.journaldunet.com" + lien + "/sante-social")
    time.sleep(2)

    if req.status_code == 200 :
        with open('dataset\\santeSocial.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            # Ciblage des tableaux : OK
            tables = soup.findAll('table', class_ = "odTable odTableAuto")
            for i in range(len(tables)):
                tousLesTr = tables[i].findAll('tr') 
                for tr in tousLesTr[1:]: 
                    cle = tr.findAll('td')[0].text
                    valeur = tr.findAll('td')[1].text
                    
                    valeur = "".join(valeur.split()) # les millier sont separé par un espace, il faut le suprimmer
                    try:
                        dico[cle] = float(valeur)
                    except : 
                        dico[cle] = valeur
                    # print(cle,":",float("".join(valeur.split()))) # verif des cels et valeurs
                    # print(cle) # Sortir mes clés 
                    
            #  Recuperation des graphique : copie
            divs = soup.findAll('div', class_="section-wrapper")
            for div in divs :
                titre_h2 = div.find('h2')
                
                # Nombre d'allocataires CAF
                if titre_h2 != None and "allocataires CAF" in titre_h2.text : # etre sure du titre
                    if div.find('script').string :                              # etre sure de la presence du script pour eviter de planter le prog
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        allocataires = json_data['series'][0]['data']
                        
                        for annee,alloc in zip(annees,allocataires):
                            try :
                                dico["nbre allocataires  (" + str(annee) + ")"] = float(alloc)
                            except :
                                dico["nbre allocataires  (" + str(annee) + ")"] = ''


                # Nombre de bénéficiaires du RSA
                if titre_h2 != None and "bénéficiaires du RSA" in titre_h2.text : # etre sure du titre
                    if div.find('script').string :                              # etre sure de la presence du script pour eviter de planter le prog
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        allocataires = json_data['series'][0]['data']
                        
                        for annee,benef in zip(annees,allocataires):
                            try :
                                dico["nbre bénéficiaires RSA (" + str(annee) + ")"] = float(benef)
                            except :
                                dico["nbre bénéficiaires RSA (" + str(annee) + ")"] = ''

                # Nombre de bénéficiaires de l'aide au logement
                if titre_h2 != None and "aide au logement" in titre_h2.text : # etre sure du titre
                    if div.find('script').string :                              # etre sure de la presence du script pour eviter de planter le prog
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        allocataires = json_data['series'][0]['data']
                        
                        for annee,benefapl in zip(annees,allocataires):
                            try :
                                dico["nbre APL (" + str(annee) + ")" ] = float(benefapl)
                            except :
                                dico["nbre APL (" + str(annee) + ")" ] = ''
                        
                                
            # Nombre de bénéficiaires des allocations familiales
                if titre_h2 != None and "aide au logement" in titre_h2.text : # etre sure du titre
                    if div.find('script').string :                              # etre sure de la presence du script pour eviter de planter le prog
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        allocataires = json_data['series'][0]['data']
                        
                        for annee,benefalloc in zip(annees,allocataires):
                            try :
                                dico["nbre Alloc Familiales (" + str(annee) + ")" ] = float(benefalloc)
                            except :
                                dico["nbre Alloc Familiales (" + str(annee) + ")" ] = ''                    
                                
            writer.writerow(dico)
            print("[Santé] : ",lien)          
            # pprint(dico)
if __name__ == "__main__":
    with Pool(20) as p :
        p.map(parse, listeLiens)






    
    
    
    
