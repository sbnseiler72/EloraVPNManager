import json

import requests

from src.logger import logger
from src import config
from src.hosts.schemas import HostType, HostResponse
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class XUI:
    def __init__(self, host: HostResponse):
        if host.type is HostType.x_ui_sanaei:
            self.api = MHSANAEI(host=host)
        elif host.type is HostType.x_ui_kafka:
            self.api = FRANZKAFKAYU(host=host)


class MHSANAEI:
    def __init__(self, host: HostResponse):
        logger.debug("Init Sanaei X-UI")
        self._host: HostResponse = host

        self._base_api_url = self._generate_base_url(
            api_path=host.api_path, ssl=self._host.master
        )
        self._login_cookies = self._get_login_cookie()
        self._csrf_token = self._fetch_csrf_token()

    def _generate_base_url(self, ssl: bool = False, api_path: str = ""):
        address = self._host.ip if self._host.domain is None else self._host.domain

        base_url = "%s://%s:%s%s" % (
            "https" if ssl else "http",
            address,
            self._host.port,
            api_path,
        )

        logger.debug(f"Base URL is: {base_url}")

        return base_url

    def _get_login_cookie(self):
        base_login_url = self._base_api_url.replace("/panel/api", "")

        # 3x-ui v3 requires a CSRF token on all unsafe (POST) requests.
        # Use a session so the pre-login session cookie is carried into the login call.
        session = requests.Session()
        csrf_token = None
        try:
            csrf_url = base_login_url + "/csrf-token"
            csrf_resp = session.get(
                csrf_url, verify=False, timeout=config.X_UI_REQUEST_TIMEOUT
            )
            if csrf_resp.status_code == 200:
                csrf_token = csrf_resp.json().get("obj")
                logger.debug(f"Pre-login CSRF token fetched: {bool(csrf_token)}")
        except Exception as e:
            logger.warn(f"Could not fetch CSRF token: {e}")

        login_url = base_login_url + "/login"
        payload = {"username": self._host.username, "password": self._host.password}
        headers = {}
        if csrf_token:
            headers["X-CSRF-Token"] = csrf_token

        logger.debug("Try login with url: " + login_url)
        req = session.post(
            login_url,
            data=payload,
            headers=headers,
            verify=False,
            timeout=config.X_UI_REQUEST_TIMEOUT,
        )
        if req.status_code == 200:
            logger.info(f"Login successful for host {self._host.name}")
        else:
            logger.warn(f"Login failed for host {self._host.name}: status {req.status_code}")
        return session.cookies

    def _fetch_csrf_token(self):
        base_url = self._base_api_url.replace("/panel/api", "")
        try:
            resp = requests.get(
                base_url + "/csrf-token",
                cookies=self._login_cookies,
                verify=False,
                timeout=config.X_UI_REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                token = resp.json().get("obj")
                logger.debug(f"Post-login CSRF token fetched: {bool(token)}")
                return token
        except Exception as e:
            logger.warn(f"Could not fetch post-login CSRF token: {e}")
        return None

    def _post_headers(self):
        headers = {"Content-type": "application/json", "Accept": "text/plain"}
        if self._csrf_token:
            headers["X-CSRF-Token"] = self._csrf_token
        return headers

    @staticmethod
    def get_account_email_prefix(inbound_key: int, email: str):
        return "%s_%s" % (inbound_key, email)

    @staticmethod
    def get_account_real_email(client_email: str):
        if client_email is None:
            return None

        email_split = client_email.split("_")

        if len(email_split) > 1:
            if client_email.find(config.TEST_ACCOUNT_EMAIL_PREFIX) > 0:
                return config.TEST_ACCOUNT_EMAIL_PREFIX + email_split[-1]
            else:
                return email_split[-1]
        else:
            return None

    def get_client_stat(self, email: str):
        try:
            url = f"{self._base_api_url}/clients/traffic/{email}"
            client_stat = requests.get(
                url,
                cookies=self._login_cookies,
                verify=False,
                timeout=config.X_UI_REQUEST_TIMEOUT,
            )
            logger.debug(f"Status code: {client_stat.status_code} for client {email}")

            if client_stat.status_code != 200:
                logger.info("Error in fetch api")
                return None
            else:
                logger.debug(f"Account info: {client_stat.text}")
                data = client_stat.json()
                obj = data["obj"]
                if obj is None:
                    logger.info(f"Account does not exist with email {email}")
                    return None
                else:
                    return obj
        except Exception as error:
            logger.warn(error)
            return None

    def reset_client_traffic(self, inbound_id: int, email: str):
        headers = self._post_headers()

        url = f"{self._base_api_url}/clients/resetTraffic/{email}"

        logger.debug(f"Final url for reset client traffic is: {url}")

        try:
            response = requests.post(
                url,
                cookies=self._login_cookies,
                verify=False,
                headers=headers,
                timeout=config.X_UI_REQUEST_TIMEOUT,
            )
            data = response.json()
            logger.debug(f"Response code: {response.status_code}")
            logger.debug(f"Response text: {response.text}")

            if response.status_code == 200 and data["success"] == True:
                return True
            else:
                return False
        except Exception as error:
            logger.warn(error)
            return False

    def reset_clients_traffic(self, inbound_id: int):
        headers = self._post_headers()

        url = f"{self._base_api_url}/clients/resetAllTraffics"

        logger.debug(f"Final url for reset clients traffic is: {url}")

        try:
            response = requests.post(
                url,
                cookies=self._login_cookies,
                verify=False,
                headers=headers,
                timeout=config.X_UI_REQUEST_TIMEOUT,
            )

            data = response.json()
            logger.debug(f"Response code: {response.status_code}")
            logger.debug(f"Response text: {response.text}")

            if response.status_code == 200 and data["success"] == True:
                return True
            else:
                return False
        except Exception as error:
            logger.warn(error)
            return False

    def delete_client(self, inbound_id: int, uuid: str):
        try:
            headers = self._post_headers()

            # New API deletes by email; look up the email from the inbound's client list.
            clients = self.get_inbound_clients(inbound_id)
            if not clients:
                logger.warn(f"No clients found in inbound {inbound_id} for uuid {uuid}")
                return False

            client_email = None
            for client in clients:
                if client.get("id") == uuid:
                    client_email = client.get("email")
                    break

            if client_email is None:
                logger.warn(f"Client with uuid {uuid} not found in inbound {inbound_id}")
                return False

            url = f"{self._base_api_url}/clients/del/{client_email}"

            logger.debug(f"Final url for delete client is: {url}")

            response = requests.post(
                url,
                cookies=self._login_cookies,
                verify=False,
                headers=headers,
                timeout=config.X_UI_REQUEST_TIMEOUT,
            )
            data = response.json()
            logger.debug(f"Response code: {response.status_code}")
            logger.debug(f"Response text: {response.text}")

            if response.status_code == 200 and data["success"] == True:
                return True
            else:
                return False
        except Exception as error:
            logger.warn(error)
            return False

    def add_client(
        self,
        inbound_id: int,
        email: str,
        uuid: str,
        data_limit: int = 0,
        expire_time: int = 0,
        ip_limit: int = 0,
        flow: str = "",
        enable: bool = True,
    ):

        try:
            headers = self._post_headers()

            url = f"{self._base_api_url}/clients/add"

            logger.debug(f"Final url for add client is: {url}")

            payload_add_client = json.dumps({
                "client": {
                    "id": uuid,
                    "flow": flow,
                    "alterId": 0,
                    "email": email,
                    "limitIp": ip_limit,
                    "totalGB": data_limit,
                    "expiryTime": expire_time,
                    "enable": enable,
                    "tgId": 0,
                    "subId": "",
                },
                "inboundIds": [inbound_id],
            })

            response = requests.post(
                url,
                cookies=self._login_cookies,
                data=payload_add_client,
                verify=False,
                headers=headers,
                timeout=config.X_UI_REQUEST_TIMEOUT,
            )

            data = response.json()

            logger.debug(f"add_client response code: {response.status_code}")

            if response.status_code == 200 and data["success"] == True:
                return True
            else:
                logger.warn(f"add_client failed: {response.status_code} {response.text[:200]}")
                return False
        except Exception as error:
            logger.warn(f"add_client error: {error}")
            return False

    def update_client(
        self,
        inbound_id: int,
        email: str,
        uuid: str,
        data_limit: int = 0,
        ip_limit: int = 0,
        flow: str = "",
        expire_time: int = 0,
        enable: bool = True,
    ):
        try:
            headers = self._post_headers()

            url = f"{self._base_api_url}/clients/update/{email}"

            logger.debug(f"Final url for update client is: {url}")

            payload_update_client = json.dumps({
                "id": uuid,
                "flow": flow,
                "alterId": 0,
                "email": email,
                "limitIp": ip_limit,
                "totalGB": data_limit,
                "expiryTime": expire_time,
                "enable": enable,
                "tgId": 0,
                "subId": "",
            })

            response = requests.post(
                url,
                cookies=self._login_cookies,
                data=payload_update_client,
                verify=False,
                headers=headers,
                timeout=config.X_UI_REQUEST_TIMEOUT,
            )
            data = response.json()

            logger.debug(f"Response code: {response.status_code}")

            if response.status_code == 200 and data["success"] is True:
                return True
            else:
                logger.warn(f"update_client failed: {response.status_code} {response.text[:200]}")
                return False
        except Exception as error:
            logger.warn(error)
            return False

    @staticmethod
    def get_client_payload(
        data_limit: int,
        email: str,
        enable: bool,
        expire_time: int,
        inbound_id: int,
        uuid: str,
        ip_limit: int = 0,
        flow: str = "",
    ) -> object:
        """
        :param data_limit Data Limit
        :rtype: object
        """
        client = {
            "id": uuid,
            "flow": flow,
            "alterId": 0,
            "email": email,
            "limitIp": ip_limit,
            "totalGB": data_limit,
            "expiryTime": expire_time,
            "enable": enable,
            "tgId": 0,
            "subId": "",
        }
        clients = [client]
        clients_object = {"clients": clients}
        payload_add_client = json.dumps(
            {"id": inbound_id, "settings": json.dumps(clients_object)}
        )
        return payload_add_client

    def get_inbound_client_stats(
        self,
        inbound_id: int,
    ):
        try:
            logger.debug(f"Get client stats from {self._host.name} inbound {inbound_id}")

            url = f"{self._base_api_url}/inbounds/get/{inbound_id}"

            response = requests.get(
                url,
                cookies=self._login_cookies,
                verify=False,
                timeout=config.X_UI_REQUEST_TIMEOUT,
            )

            data = response.json()

            if data.get("obj") is not None:
                return data["obj"].get("clientStats")

            return None
        except Exception as error:
            logger.warn(error)
            return None

    def remove_inbound_clients(
        self,
        inbound_id: int,
    ):
        remote_inbound_clients = self.get_inbound_clients(inbound_id)

        for client in remote_inbound_clients:
            client_email = client["email"]
            uuid = client["id"]
            enable = client["enable"]
            logger.info(
                f"Try to delete Email: {client_email}, UUID: {uuid}, Enable:{enable}"
            )
            account_email = self.get_account_real_email(client_email)

            if account_email is None:
                logger.warn("This client is not handle with Elora Panel! Skip!")
                continue

            deleted = self.delete_client(inbound_id=inbound_id, uuid=uuid)
            if not deleted:
                logger.error(
                    f"Error in delete account email {client_email} in this inbound!"
                )
            else:
                logger.info(
                    f"Client email {client_email} Successfully deleted in this inbound!"
                )

    def get_inbound_clients(
        self,
        inbound_id: int,
    ):
        try:
            logger.debug(f"Get clients from {self._host.name} inbound {inbound_id}")

            url = f"{self._base_api_url}/inbounds/get/{inbound_id}"

            inbound_stat = requests.get(
                url,
                cookies=self._login_cookies,
                verify=False,
                timeout=config.X_UI_REQUEST_TIMEOUT,
            )

            logger.debug(
                f"Status code: {inbound_stat.status_code} for Inbound {inbound_id}"
            )

            if inbound_stat.status_code != 200:
                logger.warn(f"Non-200 response ({inbound_stat.status_code}) for inbound {inbound_id}")
                return None

            data = inbound_stat.json()

            settings = data["obj"]["settings"]

            if settings:
                # New API returns settings as a nested object; legacy versions return a JSON string.
                setting_obj = json.loads(settings) if isinstance(settings, str) else settings
                clients = setting_obj["clients"]
                return clients
            else:
                return None
        except Exception as error:
            logger.warn(error)
            return None


class FRANZKAFKAYU:
    def __init__(self):
        logger.info("init Kafka")

    def reset_traffic(self):
        logger.info(f"Traffic reseted {self.__class__}")
