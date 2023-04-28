from dao.personsdao import PersonDAO, close_session

anders = {
    "first_name":"Anders",
    "last_name":"En",
    "company_name":"LM"
}

go = {
    "first_name":"Gio",
    "last_name":"Pe",
    "company_name":"BAE"
}
# PersonDAO.add_person(anders)
# go = PersonDAO.add_person(go)
# found = PersonDAO.get_person(go.id)
# print(f'Found person: {found.id}')
persons = PersonDAO.retrieve_all()
print(f'found: {len(persons)}')
print(f'found: {persons}')

close_session()
