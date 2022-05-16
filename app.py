from flask import request, jsonify, Flask
import os
import requests  # Get URL data
from bs4 import BeautifulSoup  # Manipulate URL data
import datetime  # useful since stock prices are IRL time-series data
from pandas import DataFrame as df  # shortening to make easier
# for manipulating data after scraping
import numpy as np
import pandas as pd
from pypfopt import risk_models
import random
from pymongo import MongoClient
from bson.objectid import ObjectId
from celery import Celery

app = Flask(__name__)



app.config.update(
    CELERY_RESULT_BACKEND='redis://:euc8R1MGBQkw5gOXMgwELc3hJWi8VYPS@redis-11559.c17.us-east-1-4.ec2.cloud.redislabs.com:11559/0',
    CELERY_BROKER_URL='redis://:euc8R1MGBQkw5gOXMgwELc3hJWi8VYPS@redis-11559.c17.us-east-1-4.ec2.cloud.redislabs.com:11559/0',
    CELERY_TASK_SERIALIZER = 'json'
)
def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    # celery = Celery(
    #     app.import_name,
    #     backend=os.environ.get("CELERY_RESULT_BACKEND"),
    #     broker=os.environ.get("CELERY_RESULT_URL")
    # )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery
# celery = Celery(app.name)
celery = make_celery(app)


def scrapePrices(date):
    url_string = "https://www.jamstockex.com/trading/trade-summary/?market=combined-market&date="
    date = str(date)[:10]
    test_page = requests.get(url_string + date)
    soup = BeautifulSoup(test_page.text, "html.parser")
    soup.prettify()  # this gives HTML text for a given web page

    rows = soup.find_all("tr")
    tickers = []
    closingPrices = []
    for row in rows:
        rowData = row.get_text().split()
        if rowData[0] != "Symbol":
            ticker = rowData[0]
            if ('USD' in rowData or "(USD)" in rowData) and "USD" not in rowData[0]:
                ticker += "USD"
            tickers.append(ticker)
            try:
                price = rowData[-3]
                if ',' in price:
                    price = price.replace(',', '')
                price = float(price)
            except ValueError:
                price = rowData[-1]
                if ',' in price:
                    price = price.replace(',', '')
                price = float(price)
            closingPrices.append(price)

    data = {"Ticker": tickers,
            date: np.array(closingPrices, dtype=object)}
    pdframe = df(data)
    return pdframe


def scrapeDates():
    date = datetime.date.today()
    date -= datetime.timedelta(days=1)
    pdframe = scrapePrices(date)
    pdframe = pdframe.set_index('Ticker')
    print(pdframe)
    for i in range(300):
        date -= datetime.timedelta(days=1)
        frame = scrapePrices(date)
        frame = frame.set_index('Ticker')
        if(len(pdframe.index) < len(frame.index)):
            pdframe = pdframe.merge(frame, on='Ticker', how='right')
        else:
            pdframe = pdframe.merge(frame, on='Ticker', how='left')
        print(pdframe)
    pdframe = pdframe.drop_duplicates()
    return pdframe.to_dict()


def scrapeDate(existing_data):
    date = datetime.date.today()
    pdframe = pd.DataFrame.from_dict(existing_data)
    pdframe.index.name = 'Ticker'
    print(pdframe)
    frame = scrapePrices(date)
    if(frame.empty):
        return pdframe.to_dict()

    frame = frame.set_index('Ticker')
    if(len(pdframe.index) < len(frame.index)):
        pdframe = pdframe.merge(frame, on='Ticker', how='right')
    else:
        pdframe = pdframe.join(frame, on='Ticker', how='left')
    print(pdframe)
    pdframe = pdframe.drop_duplicates()
    return pdframe.to_dict()


@app.route('/get-prices', methods=['GET'])
def prices():
    client = MongoClient(os.environ.get("DATABASE_URL"))
    print("Connection Successful")
    db = client.database

    prices = db.prices
    data = prices.find_one(ObjectId("627d84baa29bb4d82d3213fa"))["prices"]
    new_data = scrapeDate(data)
    query = {"_id": ObjectId("627d84baa29bb4d82d3213fa")}
    newvalues = {"$set": {"prices": new_data}}
    data = prices.update_one(query, newvalues)
    return new_data

# Route for creating portfolio


def initialize(data, n=48):
    # We create n portfolios with random weights as our initial population
    population = []
    for _ in range(n):
        population.append(list(zip(list(data.columns), np.random.dirichlet(
            np.ones(len(data.columns)), size=1).tolist()[0])))
    return population


def mutate(portfolio):
    # We swap 2 random stocks' weights
    asset1position = random.randint(0, len(portfolio)-1)
    asset2position = random.randint(0, len(portfolio)-1)
    portfolio[asset1position], portfolio[asset2position] = portfolio[asset2position], portfolio[asset1position]
    return portfolio


@app.route('/generate-portfolio', methods=['POST'])
def gen_portfolio():
    req = request.json
    age = int(req["age"])
    net_worth = float(req["net_worth"])
    salary = float(req["salary"])
    reported_risk = int(req["reported_risk"])
    id = req["id"]
    portfolio.apply_async(args=[age,net_worth,salary,reported_risk,id])
    return "<p>Process has started!</p>"


@celery.task()
def portfolio(age,net_worth,salary,reported_risk, id):
    client = MongoClient(os.environ.get("DATABASE_URL"))
    print("Connection Successful")
    db = client.database

    prices = db.prices
    data_prices = prices.find_one(
        ObjectId("627d84baa29bb4d82d3213fa"))["prices"]
    pdframe = pd.DataFrame.from_dict(data_prices)
    # pdframe = pd.read_csv("cleanData.csv").set_index("Ticker")
    pdframe.index.name = "Ticker"
    print(pdframe)

    # req = request.json
  
    # age = int(req["age"])
    # net_worth = float(req["net_worth"])
    # salary = float(req["salary"])
    # reported_risk = int(req["reported_risk"])

    def age_risk(age):
        # Returns a lower risk for younger persons since they have more time to recover from losses
        if age < 100:
            return age/100
        else:
            return 1

    # Income to net worth ratio

    def nw_income(net_worth, salary):
        # Returns a lower risk for those who rely on their salary less (ASSUMPTION)
        return salary/net_worth

    # Self-reported risk

    def reportedRisk(risk):
        # Risk is a number between 0 and 10
        # Returns a lower risk aversion to persons who have a higher risk appetite
        return (10-risk)/10

    def concentration_risk(portfolio):
        # Applying a modest concentration risk metric based on the Herfindahl–Hirschman index
        return sum([(x[1])**2 for x in portfolio])

    def risk_premium(portfolio, age, net_worth, salary, reported_risk):
        # Averages all the above functions to one risk level
        # 0.5 added to normalize output range
        return np.mean(age_risk(age) + nw_income(net_worth, salary) + reportedRisk(reported_risk)) + 0.5

    def transaction_cost(transaction_value, broker_commission=0.005):
        commission_fee = broker_commission * transaction_value
        cess = 0.003 * transaction_value
        trading_fee = 0.003 * transaction_value
        gct = 0.15 * (commission_fee + cess + trading_fee)
        return sum(commission_fee, cess, trading_fee, gct)

    def find_transaction_value(current_portfolio, new_portfolio, portfolio_value):
        # helper function to give a dollar value to the stocks that we move
        value = 0
        for i in len(current_portfolio):
            change = abs(current_portfolio[i][1] - new_portfolio[i][1])
            value += change * portfolio_value
        return value

    def expected_portfolio_value_change(current_portfolio, new_portfolio, data, portfolio_value):
        returns = data.pct_change()
        current_expected_value = np.sum(
            returns.mean() * [x[1] for x in current_portfolio] * 252)
        new_expected_value = np.sum(
            returns.mean() * [x[1] for x in new_portfolio] * 252)
        # We have two portfolios. We have to trade assets to move from one to the other.
        # We need to find the value of these transactions (trades) to then substract the costs associated.
        transaction_value = find_transaction_value(
            current_portfolio, new_portfolio, portfolio_value)
        transaction_fees = transaction_cost(transaction_value)
        net_change = new_expected_value - current_expected_value - transaction_fees
        return net_change

    def fitness(portfolio, age, net_worth, salary, reported_risk, policy_rate=0.02):
        # Simple since we abstracted almost everything away
        # The higher the output, the better
        returns, volatility, sharpe = evalPortfolio(
            portfolio, data, policy_rate)
        premium = risk_premium(portfolio, age, net_worth,
                               salary, reported_risk)
        risk_rating = volatility * \
            ((0.9 * premium) + (0.1 * concentration_risk(portfolio)))
        return returns/risk_rating

    portfolioValues = []

    def evalPortfolio(portfolio, data, policyRate=0.02):
        # Accepts a list of 2-tuples: ('ticker',weight between 0 and 1) and returns expected annual
        # return and annual volatility. May implement with dictionary
        portfolioReturn = 0
        returns = (data.iloc[-1] - data.iloc[0])/data.iloc[0]
        for asset in portfolio:
            ticker, weight = asset
            portfolioReturn += returns[ticker] * weight

        for i in range(len(data)):
            dailyPortfolioValue = sum(
                [x[1] * data.iloc[i][x[0]] for x in portfolio])
            portfolioValues.append(dailyPortfolioValue)

        #print('Return: ' + '{:.1%}'.format(portfolioReturn))

        S = risk_models.sample_cov(data)
        portfolio.sort(key=lambda x: list(data.columns).index(x[0]))
        weights = np.array([x[1]for x in portfolio])
        variance = np.dot(weights.T, np.dot(S, weights))
        volatility = np.sqrt(variance)
        #print('Volatility: ' + '{:.1%}'.format(volatility))
        sharpeRatio = (portfolioReturn - policyRate)/volatility
        #print("Sharpe Ratio: " + '{:.3}'.format(sharpeRatio))
        return (portfolioReturn, volatility, sharpeRatio)

    def select_chromosome(population):
        # Returns the best portfolio of the population
        # if half, returns the fitter half of the population
        fitness_values = list(zip(range(len(population)), [fitness(
            portfolio, age, net_worth, salary, reported_risk) for portfolio in population]))
        # We have to normalize the values now
        #normalized_fitness_values = [raw_fitness/sum(fitness_values) for raw_fitness in fitness_values]
        # print(normalized_fitness_values)

        return (population[fitness_values.index(max(fitness_values))], max(fitness_values)[1])

    def crossover(population):
        def normalize(portfolio):
            weights_sum = sum(map(lambda x: x[1], portfolio))
            return [(ticker, weight/weights_sum) for ticker, weight in portfolio]
        alpha = 0.5  # probability of parent 1's genes passing on rather than parent 2's
        cross = [0, 1]
        new_generation = []
        mating_pool = random.sample(population, len(population)//2)

        best_chromosome, fitness_val = select_chromosome(population)
        print("Fitness on this iteration")
        # requests.get("https://celery-omi-test.herokuapp.com/third")
        print(fitness_val)
        best_chromosome.sort()
        for portfolio in mating_pool:
            portfolio.sort()
            use_best = random.choices(cross, cum_weights=[70, 100])[0]
            mutation = random.choices(cross, cum_weights=[90, 100])[0]
            child1 = []
            child2 = []
            if use_best:
                for asset in range(len(portfolio)):
                    child1.append((portfolio[asset][0],
                                  alpha * portfolio[asset][1] +
                                  (1 - alpha) * best_chromosome[asset][1]))

                    child2.append((portfolio[asset][0],
                                  (1 - alpha) * portfolio[asset][1] +
                                  alpha * best_chromosome[asset][1]))
            else:
                parent1 = population[random.randint(0, len(population)-1)]
                parent1.sort()
                for asset in range(len(portfolio)):
                    child1.append((portfolio[asset][0],
                                  alpha * portfolio[asset][1] +
                                  (1 - alpha) * parent1[asset][1]))
                    child2.append((portfolio[asset][0],
                                  (1 - alpha) * portfolio[asset][1] +
                                  alpha * parent1[asset][1]))
            child1 = normalize(child1)
            child2 = normalize(child2)
            if mutation:
                child1 = mutate(child1)
                child2 = mutate(child2)
            new_generation.append(child1)
            new_generation.append(child2)
        return (new_generation, fitness_val)

    pdframe = pdframe.drop_duplicates()
    # pdframe = pdframe.replace(np.nan, 0)
    # pdframe = pdframe.ffill(axis=0)
    pdframe = pdframe.ffill(axis=1)

    data = pdframe.transpose()
    data = data.fillna(data.mean(axis=0))
    print(data)

    population = initialize(data)
    # return {"res": population}
    print("Population initialized")
    # Crossover for 16 generations
    generation_count = 0
    max_generations = 16
    fitness_list = []
    # helps to save the best population encountered in case of high local maxima
    best = [[], -1]
    while (generation_count < max_generations):
        print("Generation: " + str(generation_count))
        population, current_fitness = crossover(population)
        if current_fitness > best[1]:  # saving best so far
            best[0], best[1] = population, current_fitness

        print("Population size: " + str(len(population)))
        fitness_list.append(current_fitness)
        if len(fitness_list) > 3:  # avoids getting stuck on local maxima
            if abs(fitness_list[-1] - fitness_list[-2]) < 0.02 and\
                    abs(fitness_list[-2] - fitness_list[-3]) < 0.02:
                print("Fitness converged. Stopping iteration.")
                break
        generation_count += 1
    new_portfolio, _ = select_chromosome(best[0])

    new_portfolio.sort(key=lambda x: x[1])
    new_portfolio.reverse()
    datesAndValues = list(zip(list(data.index.values),portfolioValues))

    final_portfolio = [{"ticker": x[0], "weight": x[1]} for x in new_portfolio]
    evaluation = evalPortfolio(new_portfolio,data)
    print(evaluation)
    dates = [{"date": x[0], "value": x[1]} for x in datesAndValues]


    myobj = {"userId":id, "indices":final_portfolio, "tracker": dates}


    
    requests.post("https://foliolens-backend.herokuapp.com/portfolios/add-indices", json =myobj)

    return {
        "portfolio": final_portfolio,
        "tracker": dates
    }

if __name__ == "__main__":
    app.run()
