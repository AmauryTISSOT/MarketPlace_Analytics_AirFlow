from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    # pour que {{ ds }} soit remplace dans les requetes SQL
    template_fields = ["rules"]

    def __init__(self, rules, postgres_conn_id="postgres_dwh", **kwargs):
        super().__init__(**kwargs)
        self.rules = rules
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        resultats = []
        nb_echecs = 0

        for rule in self.rules:
            nom = rule["name"]
            sql = rule["sql"]
            print(f"verification regle: {nom}")

            # la requete doit retourner le nb de lignes en erreur
            result = pg.get_first(sql)
            nb_erreurs = result[0] if result else 0

            if nb_erreurs > 0:
                print(f"ECHEC regle '{nom}': {nb_erreurs} lignes en erreur")
                resultats.append({"name": nom, "status": "FAIL", "errors": nb_erreurs})
                nb_echecs += 1
            else:
                print(f"OK regle '{nom}'")
                resultats.append({"name": nom, "status": "OK", "errors": 0})

        print(f"resultat DQ: {len(self.rules) - nb_echecs}/{len(self.rules)} regles OK")

        # on retourne "pass" ou "fail" pour le branching
        if nb_echecs > 0:
            return "fail"
        return "pass"
