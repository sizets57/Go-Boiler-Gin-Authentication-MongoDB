import os
import requests
import logging
import json, time
import configparser
from dataclasses import dataclass, asdict
from typing import List, Dict


@dataclass
class Ticket:
    subject: str
    content: str
    team_ids: List[str]
    sub_team: List[str]
    priority_id: str
    status_id: str
    due_date: str
    dynamic_fields: List[Dict[str, str]]
    ticketCreateKey: str
    createdPlatform: str


class TicketingManager:
    def __init__(self, urls, dynamic_fields):
        # config = ConfigParser()
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - Stage %(stage)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger("ticketing_utils")

        path = os.path.join(
            os.getenv("AIRFLOW_HOME", "/opt/airflow"), f"dags/IPOM_Dags/config.ini"
        )
        # path = "dags/Astrom_Lite/config.ini"
        config = configparser.ConfigParser()

        self.logger.info(
            "Reading config file for fetching Ticketing System related data...",
            extra={"stage": "11"},
        )
        config.read(path)

        self.login_password = config["ticketing"]["password"]

        self.dynamic_fields = dynamic_fields
        self.larvel_url = urls["baseUrlLaravel"]
        self.golang_url = urls["baseUrlGo"]

    def login_ticketing(self, email):
        payload = {"email": email, "password": self.login_password}
        login_url = self.larvel_url.rstrip("/") + "/login?forData=true"
        try:
            response = requests.post(login_url, data=payload)
            self.logger.info(f"Status: {response.status_code}", extra={"stage": "12"})

            response.raise_for_status()

            content = response.json()
            token = content.get("refreshToken")

        except Exception as err:
            self.logger.error(f"Response: {response.text}", extra={"stage": "12"})
            self.logger.error(
                f"Unexpected error during login: {err}", extra={"stage": "12"}
            )
            raise

        else:
            self.header = {"Authorization": f"Bearer {token}"}

    def prepare_tickets_to_create(self, df):
        tickets = []
        try:
            for ind, row in df.iterrows():
                ticket = Ticket(
                    subject=row["subject"],
                    content=row["description"],
                    team_ids=[row["team"]],
                    sub_team=[row["sub_team"]] if row["sub_team"] is not None else [],
                    priority_id=row["priority"],
                    status_id="opened",
                    due_date=row["duedate"],
                    createdPlatform="astrum-lite",
                    dynamic_fields=[
                        {field["name"]: str(row[field["column"]])}
                        for field in self.dynamic_fields
                        if field["column"] in row
                    ],
                    ticketCreateKey="",
                )
                tickets.append(asdict(ticket))
        except Exception as err:
            self.logger.error(f"Error occured due to: {err}", extra={"stage": "13"})
        else:
            self.logger.info(
                f"Prepared {len(tickets)} tickets to create.", extra={"stage": "13"}
            )
            return tickets

    def create_ticket_golang(self, org_id, payload):
        create_url = (
            self.golang_url.rstrip("/")
            + f"/tickets/bulkCreate-tickets?forData=true&organization_id={org_id}"
        )
        max_retries = 5
        retry_delay = 10
        attempt = 0

        while attempt < max_retries:
            try:
                response = requests.post(
                    create_url, headers=self.header, data=json.dumps(payload)
                )
                status = response.status_code
                self.logger.info(
                    f"Ticket creation response: {status}", extra={"stage": "15"}
                )
                self.logger.info(
                    f"Created tickets {response.json()['created_tickets']}",
                    extra={"stage": "15"},
                )

                failed_tickets = response.json().get("failed_tickets", 0)
                failed_messages = response.json().get("failed_tickets_due", [])

                if status == 429:
                    self.logger.warning(
                        f"Rate limited. Retrying after {retry_delay} seconds...",
                        extra={"stage": "15"},
                    )
                    time.sleep(retry_delay)
                    attempt += 1
                    continue

                if status >= 400:
                    self.logger.error(
                        f"Request failed with status {status}", extra={"stage": "15"}
                    )
                    self.logger.error(
                        f"Request resposne {response.text}", extra={"stage": "15"}
                    )
                    raise Exception(
                        f"Request failed with status {status}: {response.text}",
                        extra={"stage": "15"},
                    )

                if failed_tickets > 0:
                    error_message = (
                        failed_messages[1]["message"]
                        if len(failed_messages) > 1 and "message" in failed_messages[1]
                        else "No detailed message"
                    )
                    ticket_word = (
                        "ticket was" if failed_tickets == 1 else "tickets were"
                    )

                    self.logger.warning(
                        f"{failed_tickets} {ticket_word} not created with reason: {error_message}",
                        extra={"stage": "15"},
                    )
                return response

            except Exception as err:
                self.logger.error(
                    f"Failed to create tickets on attempt {attempt + 1}: {err}",
                    extra={"stage": "15"},
                )
                attempt += 1
                time.sleep(retry_delay)

        self.logger.error(
            f"Ticket creation failed after multiple retries due to repeated errors.",
            extra={"stage": "15"},
        )
        raise RuntimeError(
            "Ticket creation failed after multiple retries due to repeated errors."
        )
