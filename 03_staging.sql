INSERT INTO staging.kunde (kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, wohnort, quelle)
   VALUES (982673, 'Drach', 'Peter', 'Herr', 'männlich', to_date('12.11.1996', 'DD.MM.YYYY'), 4, 'CRM');
INSERT INTO staging.fahrzeug (fin, kunde_id, baujahr, modell, quelle)
   VALUES ('WVW345TH6M9566671', 982673, 2003, 'Passat B6', 'Fahrzeug DB');
INSERT INTO staging.kfzzuordnung (fin, kfz_kennzeichen, quelle)
   VALUES ('WVW345TH6M9566671', 'EF-FT 953', 'Fahrzeug DB');
