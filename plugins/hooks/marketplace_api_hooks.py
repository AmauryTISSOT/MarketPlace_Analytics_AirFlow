from airflow.hooks.base import BaseHook
import requests


class MarketplaceAPIHook(BaseHook):
    """
    Hook pour interagir avec l'API Marketplace (Flask simulée).
    Gère l'authentification Bearer et les appels GET.
    """

    def __init__(self, conn_id: str = None, base_url: str = None, token: str = None):
        super().__init__()

        # Option 1: via Airflow Connection (recommandé en prod)
        self.conn_id = conn_id

        # Option 2: fallback local (dev)
        self.base_url = base_url or "http://api-simulee:5000"
        self.token = token or "my_fake_token"

        self.session = requests.Session()

    def _get_auth_headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _request(self, endpoint: str, params: dict = None):
        """
        Méthode générique GET vers l'API
        """
        url = f"{self.base_url}/{endpoint}"

        response = self.session.get(
            url,
            headers=self._get_auth_headers(),
            params=params,
            timeout=30,
        )

        response.raise_for_status()
        return response.json()

    # =========================
    # MÉTHODES MÉTIER
    # =========================

    def get_orders(self, date: str):
        """
        Récupère les commandes d'une date donnée
        """
        self.log.info(f"Fetching orders for date={date}")
        return self._request("orders", params={"date": date})

    def get_sellers(self):
        """
        Récupère tous les sellers
        """
        self.log.info("Fetching sellers")
        return self._request("sellers")

    def get_products(self):
        """
        Récupère tous les produits
        """
        self.log.info("Fetching products")
        return self._request("products")