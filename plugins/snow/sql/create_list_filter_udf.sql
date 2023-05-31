create or replace function list_filter(val array, skills_list array)
returns array
language python
runtime_version = '3.8'
handler = 'list_filter'
as
$$
def list_filter(ls, skills_list):
    cleaned_ls = [word.lower().strip() for word in ls]
    cleaned_skills = [word.lower().strip() for word in skills_list]
    new_ls = list(set([word for word in cleaned_ls if word in cleaned_skills]))
    return new_ls
$$