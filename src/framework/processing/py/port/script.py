import logging
import json
import io
from typing import Optional, Literal


import pandas as pd

import port.api.props as props
import port.slack as slack

from port.api.commands import (CommandSystemDonate, CommandUIRender, CommandSystemExit)

LOG_STREAM = io.StringIO()

logging.basicConfig(
    #stream=LOG_STREAM,
    level=logging.DEBUG,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")


def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platforms = [ 
        ("Slack", extract_slack, slack.validate), 
    ]


    # For each platform
    # 1. Prompt file extraction loop
    # 2. In case of succes render data on screen
    for platform in platforms:
        platform_name, extraction_fun, validation_fun = platform

        table_list = None

        # Prompt file extraction loop
        while True:
            LOGGER.info("Prompt for file for %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

            # Render the propmt file page
            promptFile = prompt_file("text/csv", platform_name)
            file_result = yield render_page(
                props.Translatable({"en": "Select your Slack file", "nl": "Selecteer uw Slack bestand"}),
                promptFile
            )

            if file_result.__type__ == "PayloadString":
                validation = validation_fun(file_result.value)

                # DDP is recognized: Status code zero
                if validation.status_code.id == 0: 
                    LOGGER.info("Payload for %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")

                    table_list = extraction_fun(file_result.value, validation)
                    break

                # DDP is not recognized: Different status code
                if validation.status_code.id != 0: 
                    LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                    yield donate_logs(f"{session_id}-tracking")
                    retry_result = yield render_page(
                        props.Translatable({"en": "Slack", "nl": "Slack"}),
                        retry_confirmation(platform_name)
                    )

                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        LOGGER.info("Skipped during retry %s", platform_name)
                        yield donate_logs(f"{session_id}-tracking")
                        break
            else:
                LOGGER.info("Skipped %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                break


        # Render data on screen
        if table_list is not None:
            LOGGER.info("Prompt consent; %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

            # Check if extract something got extracted
            if len(table_list) == 0:
                table_list.append(create_empty_table(platform_name))

            prompt = assemble_tables_into_form(table_list)
            consent_result = yield render_page(
                props.Translatable({"en": "Your Slack data", "nl": "Uw Slack gegevens"}),
                prompt
            )

            if consent_result.__type__ == "PayloadJSON":
                LOGGER.info("Data donated; %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                yield donate(platform_name, consent_result.value)
            else:
                LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")

    yield render_end_page()
    yield exit(0, "Success")



def assemble_tables_into_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, [])


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Er ging niks mis, maar we konden niks vinden",
       "nl": "Er ging niks mis, maar we konden niks vinden"
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table


def extract_slack(filename: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = slack.slack_logins_to_df(filename)
    if not df.empty:
        wordcloud = {
            "title": {"en": "User agent", "nl": "User agent"},
            "type": "wordcloud",
            "textColumn": "User Agent - Simple"
        }
        hours_logged_in = {
            "title": {"en": "Hours logged in by month of the year", "nl": "Uren ingelogd per maand van het jaar"},
            "type": "area",
            "group": {
                "column": "Date Accessed",
                "dateFormat": "month"
            },
            "values": [{
                "column": "Login duration in hours",
                "aggregate": "sum",
            }]
        }
        at_what_time = {
            "title": {"en": "Total time logged in by hour", "nl": "Totaal ingelogde tijd per uur van de dag"},
            "type": "bar",
            "group": {
                "column": "Date Accessed",
                "dateFormat": "hour_cycle"
            },
            "values": [{
                "column": "Login duration in hours",
                "aggregate": "sum",
            }]
        }

        table_description = props.Translatable({
            "en": "The table shows when you accessed slack from different devices, and for how long. In the first figure you can see how many hours you stayed logged in per month of the year. In the second figure you can see the hours when you are likely to be on slack. In the third figure you can see on which device you used Slack the most.",
            "nl": "De tabel toont wanneer u Slack hebt geopend vanaf verschillende apparaten en voor hoelang dat was. In de eerste grafiek kunt u zien hoeveel uur u per maand van het jaar ingelogd bent geweest. In de tweede grafiek kunt u zien op welke uren u waarschijnlijk op Slack bent geweest. In de derde grafiek kunt u zien op welk apparaat u Slack het meest hebt gebruikt.",
        })
        table_title = props.Translatable(
            {
                "en": "Your Slack access logs",
                "nl": "Uw Slack access logs"
            }
        )
        table =  props.PropsUIPromptConsentFormTable("slack", table_title, df, table_description, [hours_logged_in, at_what_time, wordcloud]) 
        tables_to_render.append(table)

    return tables_to_render


def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_page(header_text, body):
    header = props.PropsUIHeader(header_text)
    footer = props.PropsUIFooter()
    page = props.PropsUIPageDonation("slack", header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions, platform):
    description = props.Translatable(
        {
            "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a file from {platform}.",
            "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {platform} bestand heeft klik dan op “Overslaan” rechts onder."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


def exit(code, info):
    return CommandSystemExit(code, info)
