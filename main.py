import pandas as pd

data_location = "./data/"


def get_portfolio_data(data):
    client_reference = data['client reference']
    portfolios_data_path = data_location + 'portfolios.csv'
    portfolios_data = pd.read_csv(portfolios_data_path, sep=',', encoding='ansi', header=None, skiprows=1)
    for index, row in portfolios_data.iterrows():
        if client_reference == row[3]:
            try:
                data['portfolio reference'] = row[2]
            except KeyError:
                portfolios_data.close()
                print('Error Returning data from Portfolio!')
            try:
                data['account number'] = row[1]
            except KeyError:
                print('Error Returning data from Portfolio!')
            return data
    generate_error_message(data, 'Error Getting Portfolio Data', True, False)


def get_transaction_data(data):
    transaction_data_path = data_location + 'transactions.csv'
    transaction_data = pd.read_csv(transaction_data_path, sep=',', encoding='ansi', header=None, skiprows=1)
    account_number = data['account number']
    no_of_transactions = 0
    sum_of_deposits = 0
    for index, row in transaction_data.iterrows():
        if account_number == row[1]:
            no_of_transactions = no_of_transactions + 1
            if row[4] == 'DEPOSIT':
                sum_of_deposits = sum_of_deposits + row[3]
    data['number of transactions'] = no_of_transactions
    data['sum of deposits'] = sum_of_deposits
    return data


def get_account_data(data):
    accounts_data_path = data_location + 'accounts.csv'
    accounts_data = pd.read_csv(accounts_data_path, sep=',', encoding='ansi', header=None, skiprows=1)
    account_number = data['account number']
    for index, row in accounts_data.iterrows():
        if row[1] == account_number:
            data['cash balance'] = row[2]
            data['taxes paid'] = row[4]
            return data
    generate_error_message(data, 'Failed to get account data', True, True)


def generate_error_message(data, error_message, client=False, portfolio=False):
    if client and portfolio:
        message = {'Type': 'error_message', 'client_reference': data['client reference'],
                   'portfolio_reference': data['portfolio reference'], 'message': error_message}
    elif client:
        message = {'Type': 'error_message', 'client_reference': data['client reference'],
                   'portfolio_reference': 'null', 'message': error_message}
    elif portfolio:
        message = {'Type': 'error_message', 'client_reference': 'null',
                   'portfolio_reference': data['portfolio reference'], 'message': error_message}
    else:
        message = {'Fatal Error Occurred'}
    print(message)


def generate_client_message(client_reference, tax_free_allowance, taxes_paid):
    message_object = {
        'Type': 'client_message', 'client_reference': client_reference,
        'tax_free_allowance': tax_free_allowance, 'taxes_paid': taxes_paid}
    print(message_object)


def generate_portfolio_message(portfolio_reference, cash_balance, number_of_transactions, sum_of_deposits):
    message_object = {
        'Type': 'portfolio_message', 'portfolio_reference': portfolio_reference,
        'cash_balance': cash_balance, 'number_of_transaction': number_of_transactions,
        'sum_of_deposits': sum_of_deposits}
    print(message_object)


def lambda_handler():
    clients_data_path = data_location + 'clients.csv'
    client_data = pd.read_csv(clients_data_path, sep=',', encoding='ansi', header=None, skiprows=1)
    for index, row in client_data.iterrows():
        data = {}
        # Lets get the client reference
        try:
            data['client reference'] = row[3]
        except KeyError:
            generate_error_message(data, '', False, False)

        # Now we get the tax free allowance
        try:
            data['tax free allowance'] = row[4]
        except KeyError:
            generate_error_message(data, '', False, False)

        get_portfolio_data(data)
        get_transaction_data(data)
        get_account_data(data)

        generate_client_message(data['client reference'], data['tax free allowance'], data['taxes paid'])

        generate_portfolio_message(data['portfolio reference'], data['cash balance'], data['number of transactions'],
                                   data['sum of deposits'])
        data.clear()
        print('end')


lambda_handler()
