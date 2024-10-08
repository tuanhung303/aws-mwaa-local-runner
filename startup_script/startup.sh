#!/bin/sh


echo "------------------------------"
echo "Installing virtual Python env"
echo "------------------------------"

pip3 install --find-links /usr/local/airflow/plugins --no-index --upgrade pip

echo "Current Python version:"
python3 --version 
echo "..."

sudo pip3 install --user --find-links /usr/local/airflow/plugins --no-index virtualenv
sudo mkdir python3-virtualenv
cd python3-virtualenv
sudo python3 -m venv dbt-env
sudo chmod -R 777 *

echo "------------------------------"
echo "Activating venv in"
$DBT_ENV_PATH
echo "------------------------------"

source dbt-env/bin/activate
pip3 list

echo "------------------------------"
echo "Installing libraries..."
echo "------------------------------"

# do not use sudo, as it will install outside the venv
pip3 install --find-links /usr/local/airflow/plugins --no-index dbt-athena-community

echo "------------------------------"
echo "Venv libraries..."
echo "------------------------------"

pip3 list
dbt --version

echo "------------------------------"
echo "Deactivating venv..."
echo "------------------------------"

deactivate