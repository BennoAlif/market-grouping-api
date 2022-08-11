import ast
from datetime import datetime, timedelta, date

import networkx as nx
import networkx.algorithms.community as nx_comm
import pandas as pd
import tweepy
from flask import Flask, jsonify, make_response, request
from flask_cors import CORS
from flask_marshmallow import Marshmallow
from flask_sqlalchemy import SQLAlchemy
from marshmallow import fields

import louvain

BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAEkYWAEAAAAAiCZ95QEqxNKuluivi0dNKwu%2BUIA%3DpXPhzD5xrJFlCx6roDUnzjJ6jtuh8wr2AyPhfZls4g4Yo4kH8y"
client = tweepy.Client(bearer_token=BEARER_TOKEN)

# query = '(context:152.825047692124442624 OR context:66.839160129752686593 OR context:65.1256236649253449729 OR context:65.903303816698671104) lang:id'

ma = Marshmallow()

app = Flask(__name__)
CORS(app)
DB_NAME = 'db_market_grouping'
ADDRESS = f'mysql+pymysql://benno:Loking123@market-grouping.mysql.database.azure.com:3306/{DB_NAME}?charset=utf8mb4'
app.config['SQLALCHEMY_DATABASE_URI'] = ADDRESS
# app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+pymysql://root:@localhost:3306/{DB_NAME}'
# app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+pymysql://root:`c)x:%IcS9:(a]x1@34.140.36.13/{DB_NAME}?ssl_verify_identity=true'
db = SQLAlchemy(app)

# engine = create_engine(ADDRESS)

context_entities = [
    {
        "name": "Fashion",
        "context": "context:67.839543390668673024"
    },
    {
        "name": "Gadgets",
        "context": "context:131.1286204410498961408"
    },
    {
        "name": "Beauty",
        "context": "context:65.850395585941086209"
    },
    {
        "name": "Entertainment",
        "context": "context:45.781974597310615553"
    },
    {
        "name": "Video games",
        "context": "context:131.1070028159964262400"
    },
    {
        "name": "Apparel/Accessories",
        "context": "context:46.781974596715024385"
    },
    {
        "name": "Books",
        "context": "context:131.847519659179954178"
    },
    {
        "name": "Movies & TV",
        "context": "context:131.1276514328007467008"
    },
    {
        "name": "Music",
        "context": "context:131.847524658970636288"
    },
    {
        "name": "Appliances",
        "context": "context:66.864912523077615616"
    },
    {
        "name": "Wellness and health",
        "context": "context:65.903303816698671104"
    },
    {
        "name": "Sports",
        "context": "context:131.847900493514891265"
    },
    {
        "name": "Pets",
        "context": "context:131.852262932607926273"
    },
    {
        "name": "Travel",
        "context": "context:66.839160129752686593"
    },
    {
        "name": "Food",
        "context": "context:152.825047692124442624"
    },

]

# Models
class Tweet(db.Model):
    __tablename__ = "tweets"
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    text = db.Column(db.TEXT)
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


def get_data_from_api(query, start_time, end_time):
    tweets_data = []
    tweets_user = []
    query = query + " lang:id"
    print(query)
    # print(start_time, end_time)
    for response in tweepy.Paginator(client.search_recent_tweets,
                                     query=query,
                                     tweet_fields=["created_at", "text", "author_id", "entities", "in_reply_to_user_id",
                                                   "context_annotations"],
                                     user_fields=["username"],
                                     max_results=20,
                                     start_time=start_time,
                                     end_time=end_time,
                                     expansions='author_id', limit=1):
        tweets_data += response.data
        tweets_user += response.includes["users"]

    tweets_data_df = pd.DataFrame(tweets_data)
    tweets_user_df = pd.DataFrame(tweets_user)
    # print(tweets_data_df["in_reply_to_user_id"])
    tweets_data_df["in_reply_to_user_id"] = tweets_data_df["in_reply_to_user_id"].astype("Int64")
    # tweets_data_df["created_at"] = tweets_data_df["created_at"].dt.strftime('%Y-%m-%d %H:%M:%S')

    tags = []
    for i, j in enumerate(tweets_data_df["context_annotations"]):
        temp_context = []
        for context in j:
            temp_context.append(context["entity"]["name"])

        if "Fashion" in temp_context:
            tags.append("Fashion")
        elif "Gadgets" in temp_context:
            tags.append("Gadgets")
        elif "Beauty" in temp_context:
            tags.append("Beauty")
        elif "Entertainment" in temp_context:
            tags.append("Entertainment")
        elif "Video games" in temp_context:
            tags.append("Video games")
        elif "Apparel/Accessories" in temp_context:
            tags.append("Apparel/Accessories")
        elif "Books" in temp_context:
            tags.append("Books")
        elif "Movies & TV" in temp_context:
            tags.append("Movies & TV")
        elif "Music" in temp_context:
            tags.append("Music")
        elif "Appliances" in temp_context:
            tags.append("Appliances")
        elif "Wellness and health" in temp_context:
            tags.append("Wellness and health")
        elif "Sports" in temp_context:
            tags.append("Sports")
        elif "Pets" in temp_context:
            tags.append("Pets")
        elif "Travel" in temp_context or "General Travel" in temp_context:
            tags.append("Travel")
        elif "Food" in temp_context:
            tags.append("Food")
        else:
            tags.append("Other")

        tweets_data_df["context_annotations"][i] = temp_context

    tweets_data_df.insert(6, 'tag', tags)

    tweets_data_df["context_annotations"] = tweets_data_df["context_annotations"].astype("str")
    tweets_data_df["entities"] = tweets_data_df["entities"].astype("str")

    tweets_df = tweets_user_df.rename(columns={"id": "author_id"})
    tweets_df = tweets_df.drop_duplicates()
    df = tweets_data_df.merge(tweets_df, left_on='author_id', right_on='author_id')

    print("Sebelum sql")
    df.to_sql(name='tweets', con=db.engine, index=False, if_exists='append')
    print("Setelah sql")


def social_network_analysis(tweets):
    tweets_db_df = pd.DataFrame(tweets)
    in_reply_to_user_df = []
    if "in_reply_to_user_id" in tweets_db_df.keys():
        in_reply_to_user_df = tweets_db_df[tweets_db_df['in_reply_to_user_id'].notna()]

        in_reply_to_user_df = in_reply_to_user_df.merge(tweets_db_df, left_on='in_reply_to_user_id',
                                                        right_on='author_id')
        in_reply_to_user_df = in_reply_to_user_df.rename(
            columns={"username_x": "target", "username_y": "source", "context_annotations_x": "context_annotations",
                     "text_x": "text", "tag_x": 'tag'})
        in_reply_to_user_df = in_reply_to_user_df[["source", "target", "context_annotations", "text", "tag"]]
        in_reply_to_user_df = in_reply_to_user_df.drop_duplicates(keep='first', ignore_index=True)

    mentions = []

    for i in range(len(tweets_db_df)):
        if "entities" in tweets_db_df.keys():
            if (tweets_db_df["entities"][i]) != "nan":
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

    # Pakai Louvain
    communities = sorted(louvain.louvain_communities(G), key=len, reverse=True)

    print(nx_comm.modularity(G, communities))

    modularity_dict = {}
    for i, c in enumerate(
            communities):
        for name in c:
            modularity_dict[name] = i

            # Now you can add modularity information like we did the other metrics
    nx.set_node_attributes(G, modularity_dict, 'modularity')

    context = {(j["source"], j["target"]): ast.literal_eval(j["context_annotations"]) for i, j in
               final_df.iterrows()}
    nx.set_edge_attributes(G, context, "context")

    tags = {(j["source"], j["target"]): (j["tag"]) for i, j in final_df.iterrows()}
    nx.set_edge_attributes(G, tags, "tag")

    text = {(j["source"], j["target"]): j["text"] for i, j in final_df.iterrows()}
    nx.set_edge_attributes(G, text, "text")

    filtered_dict = {k: v for (k, v) in modularity_dict.items() if v < 10}

    fullgraph = nx.json_graph.node_link_data(G.subgraph(set(filtered_dict)))

    # for i in range(10):
    #     print(communities[i])
    subgraph = {"subgraph" + str(i + 1): nx.json_graph.node_link_data(G.subgraph(communities[i])) for i in range(10)}

    graph = {"fullgraph": fullgraph, "subgraph": subgraph}

    return graph


def get_context(topics):

    contexts = []
    for topic in context_entities:
        if topic["name"] in topics:
            contexts.append(topic["context"])

    return " OR ".join(contexts)


def context_to_query(topics):

    query_sql = ["tag = '" + i + "'" for i in topics]

    return " OR ".join(query_sql)


@app.route('/', methods=['GET'])
def index():  # put application's code here
    return make_response(jsonify({"data": "Hello world"}))


@app.route('/market-grouping', methods=['POST'])
def market_grouping():
    json = request.get_json()
    topics = json["topic"]
    query = get_context(topics)
    query_sql = context_to_query(topics)
    # get_tweets = Tweet.query.all()
    # tweet_schema = TweetSchema(many=True)
    # tweets = tweet_schema.dump(get_tweets)
    sql = "select date(created_at) from tweets WHERE DATE(created_at) > now() - INTERVAL 7 day and " + query_sql + " group by cast(created_at as date ) order by created_at desc;"
    print(sql)
    res = db.engine.execute(sql)

    last_date = [row[0].isoformat() for row in res]

    if not last_date:
        d = datetime.utcnow() - timedelta(days=7) + timedelta(minutes=1)
        last_date = [str(d.strftime("%Y-%m-%d"))]

    formatted_last_date = datetime.strptime(last_date[0], '%Y-%m-%d').date()
    days = 1
    start_time = datetime.combine(formatted_last_date + timedelta(days=days), datetime.min.time()).isoformat("T") + "Z"
    end_time = datetime.combine(formatted_last_date + timedelta(days=days), datetime.max.time()).isoformat("T") + "Z"

    if last_date[0] != str(date.today()):
        while True:
            try:
                get_data_from_api(query, start_time, end_time)
                days += 1
                start_time = datetime.combine(formatted_last_date + timedelta(days=days),
                                              datetime.min.time()).isoformat(
                    "T") + "Z"
                end_time = datetime.combine(formatted_last_date + timedelta(days=days) - timedelta(minutes=1),
                                            datetime.max.time()).isoformat(
                    "T") + "Z"
                db.session.commit()
            except:
                d = datetime.utcnow() - timedelta(seconds=11)
                end_time = d.isoformat("T") + "Z"
                get_data_from_api(query, start_time, end_time)
                break

    get_tweets = Tweet.query.filter(Tweet.tag.in_(topics)).all()
    tweet_schema = TweetSchema(many=True)
    tweets = tweet_schema.dump(get_tweets)

    graph_json = social_network_analysis(tweets)

    return make_response(jsonify({"data": graph_json}))


if __name__ == '__main__':
    app.run()
