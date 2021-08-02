import facebook
import json
import time
import datetime
from datetime import date
from datetime import timedelta
from azure.storage.blob import BlockBlobService

yesterday = date.today() - timedelta(1)
curr_date = str(yesterday)
print(curr_date)
storageAccountName = "marketingbidata"
storageKey = "7ZgRghttYibcCbTm5AgYk/7Seu9m2bJEI+hmpuk471RngefDdM7AKXVXSzyTjsM/SHSG4I7uuf+vxgrMUklowg=="
containerName = "marketingdataoutput"

blobService = BlockBlobService(account_name=storageAccountName,
                               account_key=storageKey
                               )

#page_token = "EAAI1UkrPCPEBAO1cOCMs89ZB6xejlVwITlayWf568tYZBTjpVoAiPlwKDYZCNnTL2KTMWpCz3kZAq3BqZCl4ZBHKu95OkZBl5fshUz2AWIdLEbdfVfsJAMZAvn0ZCNbETOBIH5mMV3tHg1FTTBB7cHDMvapuhgiNVka57wiCH4mU6nUc84r9nyoeO74xJmLQJ31MuHaSZAXtWFzP852M9TYZBmT"



# page likes = page_fans
# page unlikes = page_fans_remove   not a valid metric
# page follow  =
# page unfollow =
# page views = page_views_total
# page impressions = page_impressions
# page impression organic = page_impressions_organic
# page engagement total = page_engaged_users
# post reach =
# post reactions = page_actions_post_reactions_like_total
# page_actions_post_reactions_love_total
# page_actions_post_reactions_wow_total
# page_actions_post_reactions_haha_total
# page_actions_post_reactions_sorry_total
# page_actions_post_reactions_anger_total
# page_actions_post_reactions_total

page_ids = [ {"name":"WaterWipes Colombia" , "id": "101903678183289" , "token":"EAAI1UkrPCPEBAAeDJZBpOhvZC8tGUrzPJ8cK4q7LXeePZAEF9cOANCagfNcQSg8psgCwKganCAAoLS5O8SnQPqdhK5m4PLzngu7rI5zZCSnOWJlTbpSw3RHPB3Rn5xCEgLWL9funILZCZAhjPDwLAZBpqXq0FT5oTQcdFJtD2I1zcTBGNf3KGi0U8fkrIcAbPhbG8DgnZB3MewZDZD"},
             {"name":"WaterWipes Costa Rica", "id":"100993074986960" , "token":"EAAI1UkrPCPEBAL82LdcxmZAgJ9OJg00iSAdDu9ZCZBJJZAp0jXsqHKGnOpvb1SBAZChK64EK4EPeBN6oeSoiRzZBZBVyZAjX0UCOuODh8xgF8iMUCdHgUMUEwfFd3i6UhiZBV4EWVBpqEYEjRzlQgXgGZBdW9D1knhsm6GJloSjJAllrQZAyd1O5ErJSziPmqw4ZAdajNXY7TX3AfgZDZD"},
             {"name":"WaterWipes Panam","id": "113123710342046","token":"EAAI1UkrPCPEBACtyVZCHO93EiY7KAddzxnZBjOKY713VHSYpmffvFicQBIeOlYj6mrCbaPv5vEvLhkcYUMnB3ZCWckT624CHljcvfDmkkNg72gon3pP23pjBb3VF6HkN6SqVzaxZBUBaS84HZCoZAZCexLNu8TB76FYFMCzR429FvdZBTsKbtUKGscqZC7tYZCTYPrbXx6mCk8MQZDZD"},
             {"name":"Waterwipes(US & CA)" ,"id":"268901723174466", "token":"EAAI1UkrPCPEBAPuDixDZAi6azTNiu1aqK7rjxfhvWR051xJDVlCNk4HVOEWS4hWqSyIO7WY2ZC7xysQpxSBg056lHUF5x0SjMrZClt8RQgZC9OC3dzySgatZA3CYKwWqmeh2ZCziF3AlzKwShZBbmJaMWP2gu1EarKLO0fzTBJjpB7qGqLJ3bzr9o7p8gCKfOkYZCR0clqroDd9CWnwiDtA9"},
             {"name":"WaterWipes (Default)","id":"206933556058239", "token":"EAAI1UkrPCPEBAP0W1GKCKEskBEeBkczjDRFkeeF2ylB9JGpRAcUOTSG2d73LloNjgGMaP4sRLFaFfFPhJwatZA8DbYpTZBAjWbZBOjqokpUwjZC7VVIfONkUZAWxkOtRjXZCMLG6vZCzZAH9Fhws27PxROulAa1ptd6hadvZB8i2LtM6OXg7DFzRliyZBN2lGaZAaZCgKZCLgPBexiMZA8TTFl2ZCfv"},
             {"name":"WaterWipes (CL)","id":"699426210417599","token":"EAAI1UkrPCPEBAEqZBTWUxULqh6KIcUM1VVSckHZCZBfkEWzSychMowdARB8dmjPpdFkxNyqnjPhc0db24LMRHiYsMT16hhim09wwVJZAN0iJP7N35EuuH8BmoTNVY1ys8myz5aSTso2WYDzejHBnjaW0yzATMitmGsJUi77riI0ET1eGCK5O4JRHohG30R99E3ZCUX5yb8AZDZD"},
             {"name":"WaterWipes (ES)","id":"2577584698930176","token":"EAAI1UkrPCPEBAPUGgknzgi6pTJT1Iwg7QWjg3eHcufVKus9ltfAoN8d4EMHRDX3yvYVA6Y5wL9wqsEXbI1Wi6t3QbZAWczhSNFZBmfZAMmoxsjf5AgN2SZBOlkwiBpzHVcwYbzXpEzLEV2lGR0y0c9dXL6cEJpIZAg1EX0oEFZAfh1XcXlYAXaklP6VmKL0vSt90fyXaZC3OAZDZD"},
             {"name":" WaterWipes (PT)","id":"881712535199569","token":"EAAI1UkrPCPEBAGGDZBY6aCVhqT4KphotbUdYxAEMawq8KMic4ywwthud20wFmARj1kEyOfVBeqCrV6hu7j8kV35oerMU3x8ghm2nWSUZBZAyBZAnwmuujoDIJub5h9Ml8MnOHjHSK62JjkQyAxM5c9NpY2ouwyTIWwsbNygTpBnU0T7Eg8AkLGPd2luD3LcNk8UZAsO6sVgZDZD"},
             {"name":"WaterWipes Belgie/Belgique","id":"360411041139529","token":"EAAI1UkrPCPEBAH3xxhwcPzxkdCOdCOQtwdZAyGpKyRwiY1EmbLgQr037Vwn8ZBkKls6q6pvyrVORG7BbE1N5iip5mNggEdayOZCz8hKfOhyd0z4O8z03C7EZBHnx3h6OfAFC0NfbPCuFNRbmLMPdSX47Tz7jcpwGMcZCtJOiZARZCnKT9R3ZCRXmooqLK34AkR8iN073p5DDjo6yYDpmqlLk"}]
metrics_tg = [ {"metr": "page_fans" ,"name":"page_likes" },{"metr": "page_fan_removes","name":"page_dislikes" },{"metr":"page_views_total","name":"page_views"},{"metr":"page_impressions","name":"page_impressions"},{"metr":"page_impressions_organic","name":"page_impressions_organic"},{"metr":"page_engaged_users","name":"page_engagement_total"},{"metr":"page_actions_post_reactions_like_total","name":"page_reactions"},
               {"metr":"page_actions_post_reactions_love_total","name":"post_reaction_love"},{"metr":"page_actions_post_reactions_wow_total","name":"post_reaction_wow"},{"metr":"page_actions_post_reactions_haha_total","name":"post_reaction_haha"},{"metr":"page_actions_post_reactions_sorry_total","name":"post_reaction_sorry"},
               {"metr":"page_impressions_unique","name":"reach"},
               {"metr":"page_impressions_organic_unique","name":"organic reach"},
               {"metr":"page_impressions_paid_unique","name":"paid reach"},
               {"metr":"page_impressions_viral_unique","name":"viral reach"},
               {"metr":"page_actions_post_reactions_anger_total","name":"post_reaction_anger"}]

page_name , page_id, metric_tag , value ,date_time = [],[],[],[],[]

for ids in page_ids:
    page_token = ids['token']
    graph = facebook.GraphAPI (access_token=page_token,
                               version="3.1")

    default_info = graph.get_object (id=ids['id'])
    print(ids['id'])
    for metric in metrics_tg:
        print("metrics = " , metric["metr"])
        page_metrics = graph.get_connections(id=ids['id'],
                                             connection_name='insights',
                                             metric=metric["metr"],
                                             date_preset= 'lifetime',
                                             period='day',
                                             show_description_from_api_doc=True)


        page_name.append(ids['name'])
        page_id.append(ids['id'])
        print(page_metrics)
        metric_tag.append(metric["name"])
        if page_metrics['data'][0]['values'][0]['value']:
            value.append(page_metrics['data'][0]['values'][0]['value'])
        else:
            value.append("0")

        date_time.append(curr_date)
        print(page_metrics)



final_json = [{"page_name": a, "page_id": b  ,"metric_tag" : c ,"value" : d ,"date_time":e} for
                a, b , c , d , e  in zip(page_name, page_id , metric_tag ,value , date_time)]
print(final_json)

blobService.create_blob_from_text(containerName, "Facebook_organic.json",
                                  json.dumps(final_json))