import ast

import networkx as nx
import networkx.algorithms.community as nx_comm
import pandas as pd
import tweepy
from flask import Flask, jsonify, make_response
from flask_marshmallow import Marshmallow
from flask_sqlalchemy import SQLAlchemy
from marshmallow import fields

BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAEkYWAEAAAAAiCZ95QEqxNKuluivi0dNKwu%2BUIA%3DpXPhzD5xrJFlCx6roDUnzjJ6jtuh8wr2AyPhfZls4g4Yo4kH8y"
client = tweepy.Client(bearer_token=BEARER_TOKEN)

query = '(context:152.825047692124442624 OR context:66.839160129752686593 OR context:65.1256236649253449729 OR context:65.903303816698671104) lang:id'

ma = Marshmallow()

app = Flask(__name__)
DB_NAME = 'db_market_grouping'
app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+pymysql://root:@localhost:3306/{DB_NAME}'
db = SQLAlchemy(app)


# Models
class Tweet(db.Model):
    __tablename__ = "tweets"
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    text = db.Column(db.String(255))
    username = db.Column(db.String(15))
    name = db.Column(db.String(50))
    context_annotations = db.Column(db.TEXT)
    tag = db.Column(db.String(25))
    entities = db.Column(db.TEXT)
    in_reply_to_user_id = db.Column(db.BigInteger)
    author_id = db.Column(db.BigInteger)
    created_at = db.Column(db.DATETIME)

    def create(self):
        db.session.add(self)
        db.session.commit()
        return self

    def __init__(self, id, text, username, name, context_annotations, tag, entities, in_reply_to_user_id, author_id,
                 created_at):
        self.id = id
        self.text = text
        self.username = username
        self.name = name
        self.context_annotations = context_annotations
        self.tag = tag
        self.entities = entities
        self.in_reply_to_user_id = in_reply_to_user_id
        self.author_id = author_id
        self.created_at = created_at

    # def __repr__(self):
    #     return '' % self.id


db.create_all()


class TweetSchema(ma.Schema):
    class Meta(ma.Schema.Meta):
        model = Tweet
        sqla_session = db.session

    id = fields.Number()
    text = fields.String()
    username = fields.String()
    name = fields.String()
    context_annotations = fields.String()
    tag = fields.String()
    entities = fields.String()
    in_reply_to_user_id = fields.Number()
    author_id = fields.Number()
    created_at = fields.DateTime()


@app.route('/', methods=['GET'])
def index():  # put application's code here
    get_tweets = Tweet.query.all()
    tweet_schema = TweetSchema(many=True)
    tweets = tweet_schema.dump(get_tweets)
    return make_response(jsonify({"data": tweets}))


@app.route('/market-grouping', methods=['GET'])
def market_grouping():
    tweets_data = []
    tweets_user = []

    for response in tweepy.Paginator(client.search_recent_tweets,
                                     query=query,
                                     tweet_fields=["created_at", "text", "author_id", "entities", "in_reply_to_user_id",
                                                   "context_annotations"],
                                     user_fields=["username"],
                                     max_results=100,
                                     expansions='author_id', limit=100):
        tweets_data += response.data
        tweets_user += response.includes["users"]

    tweets_data_df = pd.DataFrame(tweets_data)
    tweets_user_df = pd.DataFrame(tweets_user)

    tweets_data_df["in_reply_to_user_id"] = tweets_data_df["in_reply_to_user_id"].astype("Int64")
    # tweets_data_df["created_at"] = tweets_data_df["created_at"].dt.strftime('%Y-%m-%d %H:%M:%S')

    tags = []
    for i, j in enumerate(tweets_data_df["context_annotations"]):
        temp_context = []
        for context in j:
            temp_context.append(context["entity"]["name"])
        if "Fashion & beauty" in temp_context:
            tags.append("Fashion & beauty")
        elif "Travel" in temp_context or "General Travel" in temp_context:
            tags.append("Travel")
        elif "Food" in temp_context:
            tags.append("Food")
        elif "Wellness and health" in temp_context:
            tags.append("Wellness and health")
        else:
            tags.append("Other")
        tweets_data_df["context_annotations"][i] = temp_context

    tweets_data_df.insert(6, 'tag', tags)

    tweets_data_df["context_annotations"] = tweets_data_df["context_annotations"].astype("str")
    tweets_data_df["entities"] = tweets_data_df["entities"].astype("str")

    tweets_df = tweets_user_df.rename(columns={"id": "author_id"})
    tweets_df = tweets_df.drop_duplicates()
    df = tweets_data_df.merge(tweets_df, left_on='author_id', right_on='author_id')
    df.to_sql(name='tweets', con=db.engine, index=False, if_exists='append')
    get_tweets = Tweet.query.all()
    tweet_schema = TweetSchema(many=True)
    tweets = tweet_schema.dump(get_tweets)

    tweets_db_df = pd.DataFrame(tweets)

    in_reply_to_user_df = tweets_db_df[tweets_db_df['in_reply_to_user_id'].notna()]

    in_reply_to_user_df = in_reply_to_user_df.merge(tweets_db_df, left_on='in_reply_to_user_id', right_on='author_id')
    in_reply_to_user_df = in_reply_to_user_df.rename(
        columns={"username_x": "target", "username_y": "source", "context_annotations_x": "context_annotations",
                 "text_x": "text", "tag_x": 'tag'})
    in_reply_to_user_df = in_reply_to_user_df[["source", "target", "context_annotations", "text", "tag"]]
    in_reply_to_user_df = in_reply_to_user_df.drop_duplicates(keep='first', ignore_index=True)

    mentions = []

    for i in range(len(tweets_db_df)):
        # if isinstance((tweets_db_df["entities"][i]), str) or tweets_db_df["entities"][0] != "nan":
        if (tweets_db_df["entities"][i]) != "nan":
            # print((tweets_db_df["entities"][i]) != "nan")
            if "mentions" in eval(tweets_db_df["entities"][i]).keys():
                mention = {
                    "id": tweets_db_df["id"][i],
                    "mention_username": eval(tweets_db_df["entities"][i]).get("mentions")[0].get("username")
                }
                mentions.append(mention)

    mentions_df = pd.DataFrame(mentions)

    tweets_mention_df = mentions_df.merge(tweets_db_df, left_on='id', right_on='id')

    user_mentions_df = tweets_mention_df.rename(columns={"username": "source", "mention_username": "target"})
    user_mentions_df = user_mentions_df[["source", "target", "context_annotations", "text", "tag"]]
    user_mentions_df = user_mentions_df.drop_duplicates(keep='first', ignore_index=True)

    final_df = pd.concat([in_reply_to_user_df, user_mentions_df], ignore_index=True)

    G = nx.Graph()

    G = nx.from_pandas_edgelist(final_df, 'source', 'target')

    communities = sorted(nx_comm.louvain_communities(G), key=len, reverse=True)
    nx_comm.modularity(G, communities)

    modularity_dict = {}
    for i, c in enumerate(
            communities):
        for name in c:
            modularity_dict[name] = i

            # Now you can add modularity information like we did the other metrics
    nx.set_node_attributes(G, modularity_dict, 'modularity')

    context = {(j["source"], j["target"]): ast.literal_eval(j["context_annotations"]) for i, j in final_df.iterrows()}
    nx.set_edge_attributes(G, context, "context")

    tags = {(j["source"], j["target"]): (j["tag"]) for i, j in final_df.iterrows()}
    nx.set_edge_attributes(G, tags, "tag")

    text = {(j["source"], j["target"]): j["text"] for i, j in final_df.iterrows()}
    nx.set_edge_attributes(G, text, "text")

    filtered_dict = {k: v for (k, v) in modularity_dict.items() if v < 10}

    d = nx.json_graph.node_link_data(G.subgraph(set(filtered_dict)))

    return make_response(jsonify({"data": d}))


if __name__ == '__main__':
    app.run()