from dao.personsdao import PersonDAO, LocationDAO, close_session

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
#found = PersonDAO.get_person(go.id)
#print(f'Found person: {found.id}')

#38.978802771336504, -77.524484206581
# loc = {
#     "person_id":go.id,
#     "latitude":"-77.524484206581",
#     "longitude":"38.978802771336504"
# }
#LocationDAO.create(loc)

#persons = PersonDAO.retrieve_all()
#print(f'found: {len(persons)}')
person = PersonDAO.get_person(2)
print(f'found: {person}')
l = LocationDAO.retrieve(1)
print(f'found loc: {l}')

close_session()
