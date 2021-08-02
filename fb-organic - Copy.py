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

page_ids = [ {"name":"pagename1" , "id": "10xxxxxxx89" , "token":"zyx"},
             {"name":"pagename2", "id":"10xxxxxxxx60" , "token":"xyz"}
             ]
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