"""Experimental evidence-based prioritization. No I/O or notification decisions."""
import copy
from datetime import datetime, timezone
import re

from job_monitor.screening import first_publication, normal_phrase
from job_monitor.serialization import fingerprint


SCORER_VERSION = "rules-0.1-experimental"
BANDS = ("Review first", "Possible fit", "Stretch", "Insufficient evidence")
CONTEXT = {"professional", "professional_internship", "project", "study", "declaration"}


def phrase_in(text, phrase):
    return bool(re.search(r"(?<!\w)" + re.escape(normal_phrase(phrase)) + r"(?!\w)", normal_phrase(text)))


def validate_candidate(profile):
    """Validate executable fields; narrative/extension fields remain permitted."""
    if not isinstance(profile, dict) or type(profile.get("schema_version")) is not int or profile["schema_version"] != 1:
        raise ValueError("Candidate profile requires schema_version 1")
    if type(profile.get("profile_revision")) is not int or profile["profile_revision"] < 1:
        raise ValueError("Candidate profile_revision must be a positive integer")

    def strings(values, field, *, nonempty=False):
        if not isinstance(values, list) or (nonempty and not values) or any(
                not isinstance(item, str) or not item.strip() or len(item) > 2000 for item in values):
            raise ValueError(f"Invalid candidate field: {field}")

    groups = profile.get("evidence_groups")
    capabilities = profile.get("capabilities")
    if not isinstance(groups, list) or not groups or not isinstance(capabilities, list):
        raise ValueError("Candidate needs evidence_groups and capabilities lists")
    group_ids = set()
    group_contexts = {}
    for group in groups:
        if not isinstance(group, dict) or not isinstance(group.get("id"), str) or not group["id"] or group["id"] in group_ids:
            raise ValueError("Evidence group IDs must be unique nonempty strings")
        group_ids.add(group["id"])
        if 'context' in group:
            group_contexts[group['id']] = group['context']
    ids = set()
    for capability in capabilities:
        if not isinstance(capability, dict):
            raise ValueError("Invalid capability")
        for field in ("id", "name", "evidence", "support_level"):
            if not isinstance(capability.get(field), str) or not capability[field].strip():
                raise ValueError(f"Capability needs {field}")
        if capability["id"] in ids or capability.get("context") not in CONTEXT:
            raise ValueError("Duplicate capability ID or unsupported evidence context")
        ids.add(capability["id"])
        strings(capability.get("keywords"), "capability keywords", nonempty=True)
        refs = capability.get("evidence_group_ids")
        strings(refs, "capability evidence_group_ids", nonempty=True)
        if not set(refs) <= group_ids:
            raise ValueError("Capability references an unknown evidence group")
        if any(ref in group_contexts and group_contexts[ref] != capability['context'] for ref in refs):
            raise ValueError("Capability context disagrees with its source evidence group")
        if capability["support_level"] not in {"demonstrated_in_resume", "coursework_only", "listed_only"}:
            raise ValueError("Unsupported capability support_level")
    strings(profile.get("career_interests", []), "career_interests")
    preferences = profile.get("matching_preferences")
    if not isinstance(preferences, dict):
        raise ValueError("Candidate needs matching_preferences")
    title_groups = preferences.get("preferred_title_groups")
    if not isinstance(title_groups, list):
        raise ValueError("Candidate needs preferred_title_groups")
    title_ids = set()
    for group in title_groups:
        if not isinstance(group, dict) or not isinstance(group.get("id"), str) or not group['id'] or group["id"] in title_ids:
            raise ValueError("Invalid or duplicate title group")
        title_ids.add(group["id"])
        strings(group.get("any_of"), "title group any_of", nonempty=True)
    seniority = preferences.get("seniority", {})
    if not isinstance(seniority, dict):
        raise ValueError("Invalid seniority preferences")
    for key in ("exclude_explicit_title_phrases", "exceptions", "review_ambiguous"):
        strings(seniority.get(key, []), "seniority " + key)
    cohort = preferences.get("cohort", {})
    if not isinstance(cohort, dict) or type(cohort.get("candidate_latest_graduation_year")) is not int:
        raise ValueError("Candidate needs a graduation year")
    years = cohort.get("excluded_intake_years", [])
    if not isinstance(years, list) or any(type(year) is not int or not 1900 <= year <= 2200 for year in years):
        raise ValueError("Invalid excluded intake years")
    targets = profile.get("target_organizations", {})
    if not isinstance(targets, dict):
        raise ValueError("Invalid target organizations")
    organizations = targets.get("organizations", [])
    if not isinstance(organizations, list):
        raise ValueError("Invalid target organizations")
    aliases, org_ids = {}, set()
    for org in organizations:
        if not isinstance(org, dict) or not isinstance(org.get("id"), str) or org["id"] in org_ids:
            raise ValueError("Invalid or duplicate organization ID")
        org_ids.add(org["id"])
        strings([org.get("name")], "organization name", nonempty=True)
        strings(org.get("aliases", []), "organization aliases")
        strings(org.get("parent_employers_for_team_review", []), "parent employer aliases")
        for alias in [org["name"], *org.get("aliases", []), *org.get("parent_employers_for_team_review", [])]:
            key = normal_phrase(alias)
            if key in aliases and aliases[key] != org["id"]:
                raise ValueError("Organization aliases identify more than one target")
            aliases[key] = org["id"]
    declared = profile.get("declared_skills", {})
    if not isinstance(declared, dict):
        raise ValueError("Invalid declared_skills")
    for field in ("languages", "ml_and_ai", "application_and_scientific", "cloud_and_systems"):
        strings(declared.get(field, []), "declared_skills " + field)
    if declared and declared.get("evidence_group_id") not in group_ids:
        raise ValueError("Declared skills reference an unknown evidence group")
    fallback = preferences.get('ai_team_fallback', {})
    aliases = preferences.get('training_keyword_aliases', {})
    if not isinstance(fallback, dict) or not isinstance(aliases, dict):
        raise ValueError("Invalid AI context/interest aliases")
    strings(fallback.get('positive_context', []), 'AI context phrases')
    for key in ('pre-training', 'post-training'):
        strings(aliases.get(key, []), 'training interest aliases')
    summary = profile.get('candidate_summary', {})
    if not isinstance(summary, dict):
        raise ValueError("Invalid candidate_summary")
    years = summary.get('professional_experience_years_approximate')
    if years is not None and (type(years) not in (int, float) or not 0 <= years <= 80):
        raise ValueError("Invalid recorded professional experience")
    return profile


def requirements(description):
    """Bounded lexical parsing; uncertain language is not a verified contract."""
    section = "unclear"
    result = []
    for line in description.splitlines():
        heading = normal_phrase(line.strip(" #*:-\t"))
        if heading in {"requirements", "qualifications", "minimum qualifications", "required qualifications",
                       "what you bring", "what you'll bring", "what you need", "about you", "must have"}:
            section = "required"
            continue
        if heading in {"preferred qualifications", "preferred", "nice to have", "bonus", "desirable"}:
            section = "desirable"
            continue
        if heading in {"about us", "about the company", "our company", "benefits", "our stack", "technology stack"}:
            section = "background"
            continue
        if heading in {"responsibilities", "what you'll do", "what you will do", "your role", "the role"}:
            section = "responsibility"
            continue
        for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z])|;\s*|\s+(?:but|however)\s+", line, flags=re.I):
            sentence = sentence.strip(" *•-\t")
            if not sentence:
                continue
            kind = section
            lower = sentence.casefold()
            if section != "background" and re.search(r"\b(?:preferred|desirable|nice.to.have|a plus|bonus)\b", lower):
                kind = "desirable"
            elif section != "background" and re.search(
                    r"\b(?:must|required|essential|minimum|you have|you bring|experience (?:with|in)|proficien(?:t|cy))\b", lower):
                kind = "required"
            elif section == "unclear" and re.search(r"\b(?:you will|you'll|build|develop|evaluate|train|deploy)\b", lower):
                kind = "responsibility"
            result.append((sentence[:2000], kind))
    return result


def asserted(sentence, term):
    """Conservative local negation handling, not natural-language understanding."""
    normalized, keyword = normal_phrase(sentence), normal_phrase(term)
    for match in re.finditer(r"(?<!\w)" + re.escape(keyword) + r"(?!\w)", normalized):
        if keyword == 'c' and normalized[match.end():].startswith(('++', '#')):
            continue
        before, after = normalized[max(0, match.start() - 45):match.start()], normalized[match.end():match.end() + 55]
        if re.search(r"\b(?:no|not|without|don't|do not|isn't|is not)\s+(?:(?:prior|any|professional|production|use|using|need|require)\s+|experience\s+(?:in|with)\s+)*$", before):
            continue
        if re.match(r"\s*(?:experience\s+)?(?:is\s+)?(?:not (?:required|necessary|needed)|isn't required|optional)\b", after):
            continue
        if re.search(r"\b(?:no|not)\s+(?:experience\s+(?:with|in)\s+)?$", before) or re.match(
                r"\s*(?:experience\s+)?(?:is\s+)?(?:not|isn't)\s+(?:required|necessary|needed)\b", after):
            continue
        negative = re.search(r"\bnot\s+(?:required|necessary|needed|used|expected)\b", after)
        if negative and not re.search(r"\b(?:required|essential|must)\b", after[:negative.start()]):
            continue
        return True
    return False


def cohort_conflicts(job, clauses, cohort):
    excluded = cohort.get("excluded_intake_years", [])
    conflicts, unknowns = [], []
    graduation = cohort["candidate_latest_graduation_year"]
    metadata_year = job.get("cohort_year")
    if metadata_year is not None and type(metadata_year) is not int:
        raise ValueError("cohort_year must be an integer when provided")
    if metadata_year in excluded:
        conflicts.append({"code": "cohort", "excerpt": f"Explicit cohort_year: {metadata_year}"})
    for text, kind in [(job["title"], "title"), *clauses]:
        normalized = re.sub(r'\s*:\s*', ' ', normal_phrase(text))
        for year in excluded:
            yr = str(year)
            if (kind == 'title' and re.search(r'\b' + yr + r'\b', normalized)
                    and re.search(r'\b(?:grad|graduate|graduates|early career|intake|cohort|program|programme|scheme)\b', normalized)
                    and not re.search(r'\bnot\s+' + yr + r'\b', normalized)):
                conflicts.append({'code': 'cohort', 'excerpt': text[:300]})
                continue
            intake = (re.search(r"\b" + yr + r"\s+(?:(?:graduate|new grad|early career|summer|winter|fall|autumn|spring)\s+){0,3}(?:intake|cohort|program|programme|graduate|graduates)\b", normalized)
                      or re.search(r"\b(?:intake|cohort|program|programme|start date|starting|starts)\s*(?:for|in|:)?\s*" + yr + r"\b", normalized))
            if intake and not re.search(r"\b(?:not|isn't|is not)\b", normalized[max(0, intake.start() - 20):intake.start()]):
                conflicts.append({"code": "cohort", "excerpt": text[:300]})
                continue
            if kind not in {"required", "title"} or not re.search(r"\bgraduat(?:e|es|ed|ing|ion)\b", normalized):
                continue
            years = [int(value) for value in re.findall(r"\b20\d{2}\b", normalized)]
            if year not in years:
                continue
            if len(years) >= 2:
                interval = bool(re.search(r'\b(?:between|from)\b|20\d{2}\s*[-–]\s*20\d{2}', text, re.I))
                compatible = min(years) <= graduation <= max(years) if interval else graduation in years
                if not compatible:
                    unknowns.append("Advert graduation window does not include the recorded MSc year; verify eligibility.")
            elif re.search(r"\bgraduat(?:e|ed|ing|ion)\s+(?:(?:date|year|in|by|of|class|:|must|be)\s+){0,3}" + yr + r"\b", normalized):
                conflicts.append({"code": "cohort", "excerpt": text[:300]})
    return list({(item['code'], item['excerpt']): item for item in conflicts}.values()), unknowns


class Evaluator:
    """One deterministic strategy, prepared once and reusable without mutable jobs."""
    def __init__(self, profile):
        self.profile = copy.deepcopy(validate_candidate(profile))
        self.profile_hash = fingerprint(self.profile)

    def assess(self, job, company, *, now=None):
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise ValueError("Assessment time needs a timezone")
        if not isinstance(job, dict) or not isinstance(company, str) or not company.strip():
            raise ValueError("Posting needs a job object and an employer")
        for key in ("title", "description", "url", "location"):
            if not isinstance(job.get(key), str):
                raise ValueError(f"Posting needs text field: {key}")
        if not job['title'].strip() or not job['url'].startswith('https://') or len(job['description']) > 100_000:
            raise ValueError("Posting needs a title, HTTPS URL and description of at most 100000 characters")
        profile = self.profile
        preferences = profile['matching_preferences']
        def discovery_terms(term):
            return [term, *preferences.get('training_keyword_aliases', {}).get(term, [])]
        clauses = requirements(job['description'])
        conflicts, unknowns = cohort_conflicts(job, clauses, preferences['cohort'])
        uncertain_requirement = bool(unknowns)
        seniority = preferences.get('seniority', {})
        for text in (job['title'], job.get('job_level', '')):
            if not isinstance(text, str):
                raise ValueError("job_level must be text")
            normalized = normal_phrase(text)
            staff_exception = any(phrase_in(normalized, value) for value in seniority.get('exceptions', []))
            for phrase in seniority.get('exclude_explicit_title_phrases', []):
                if normal_phrase(phrase) == 'staff' and staff_exception:
                    continue
                if phrase_in(normalized, phrase):
                    conflicts.append({'code': 'seniority', 'excerpt': text[:300]})
                    break
        for phrase in seniority.get('review_ambiguous', []):
            if phrase_in(job['title'], phrase):
                unknowns.append('Role level needs review: ' + phrase)
                uncertain_requirement = True
        matched_titles = [group['id'] for group in preferences['preferred_title_groups']
                          if any(phrase_in(job['title'], term) for term in group['any_of'])]
        org = next((org for org in profile.get('target_organizations', {}).get('organizations', [])
                    if any(normal_phrase(company) == normal_phrase(alias) for alias in
                           [org['name'], *org.get('aliases', [])])), None)
        parent = next((item for item in profile.get('target_organizations', {}).get('organizations', [])
                       if any(normal_phrase(company) == normal_phrase(alias) for alias in
                              item.get('parent_employers_for_team_review', []))), None)
        team = job.get('team', '')
        if not isinstance(team, str):
            raise ValueError("team must be text")
        if org is None and parent:
            if any(phrase_in(team, alias) for alias in [parent['name'], *parent.get('aliases', [])]):
                org = parent
            else:
                unknowns.append('Parent employer matches a target, but the named AI unit/team is unverified.')
        context_terms = preferences.get('ai_team_fallback', {}).get('positive_context', [])
        ai_context = (any(phrase_in(job['title'] + ' ' + team, alias) for term in context_terms for alias in discovery_terms(term))
                      or any(value in matched_titles for value in ('ai-engineer', 'ml-engineer'))
                      or any(kind == 'responsibility' and any(asserted(sentence, alias) for term in context_terms for alias in discovery_terms(term))
                             for sentence, kind in clauses))
        if not ai_context:
            unknowns.append('AI hiring-team context is not established by this description; retain for review.')
        strengths, weaker, matched_groups = [], [], {}
        supported_terms = set()
        term_contexts = {}
        for capability in profile['capabilities']:
            hits = []
            for term in capability['keywords']:
                hit = next(((sentence, kind) for sentence, kind in clauses
                            if kind in {'required', 'desirable', 'responsibility'} and asserted(sentence, term)), None)
                if hit:
                    hits.append({'term': term, 'excerpt': hit[0][:300], 'requirement_kind': hit[1]})
            if not hits:
                continue
            evidence = {'capability_id': capability['id'], 'name': capability['name'],
                        'context': capability['context'], 'support_level': capability['support_level'],
                        'evidence_group_ids': capability['evidence_group_ids'],
                        'candidate_evidence': capability['evidence'], 'matches': hits}
            if capability['support_level'] == 'demonstrated_in_resume':
                strengths.append(evidence)
                supported_terms.update(normal_phrase(hit['term']) for hit in hits)
                for hit in hits:
                    term_contexts.setdefault(normal_phrase(hit['term']), set()).add(capability['context'])
                for group in capability['evidence_group_ids']:
                    matched_groups[group] = capability['context']
            else:
                weaker.append(evidence)
        declared = profile.get('declared_skills', {})
        listed = []
        for key in ('languages', 'ml_and_ai', 'application_and_scientific', 'cloud_and_systems'):
            for term in declared.get(key, []):
                if normal_phrase(term) in supported_terms:
                    continue
                hit = next(((sentence, kind) for sentence, kind in clauses
                            if kind in {'required', 'desirable', 'responsibility'} and asserted(sentence, term)), None)
                if hit:
                    listed.append({'term': term, 'excerpt': hit[0][:300],
                                   'evidence_group_id': declared['evidence_group_id'],
                                   'status': 'Listed skill; demonstrated use/depth not established for this requirement'})
        interests = []
        for term in profile.get('career_interests', []):
            hit = next((sentence for sentence, kind in clauses if kind != 'background' and any(asserted(sentence, alias) for alias in discovery_terms(term))), None)
            if hit or any(phrase_in(job['title'], alias) for alias in discovery_terms(term)):
                interests.append({'interest': term, 'excerpt': (hit or job['title'])[:300],
                                  'status': 'Career interest, not evidence of experience'})
        gaps = []
        known_terms = {term for cap in profile['capabilities'] for term in cap['keywords']}
        known_terms.update(term for key in ('languages', 'ml_and_ai', 'application_and_scientific', 'cloud_and_systems')
                           for term in declared.get(key, []))
        known_terms.update({'Kubernetes', 'RLHF', 'DPO', 'distributed training', 'pre-training', 'PhD'})
        for sentence, kind in clauses:
            if kind != 'required':
                continue
            # An OR requirement is a choice, not evidence that every alternative is mandatory.
            disjunction = bool(re.search(r'\bor\b|and/or', sentence, re.I))
            for term in sorted(known_terms, key=str.casefold):
                if normal_phrase(term) not in supported_terms and asserted(sentence, term):
                    gaps.append({'term': term, 'excerpt': sentence[:300],
                                 'status': 'Alternative/compound requirement; verify' if disjunction else 'Direct supporting evidence not recorded',
                                 'uncertain_alternative': disjunction})
                elif (asserted(sentence, term) and normal_phrase(term) in term_contexts
                      and not term_contexts[normal_phrase(term)] & {'professional', 'professional_internship'}
                      and re.search(r'\b(?:production|professional|commercial|industry)\b', sentence, re.I)):
                    gaps.append({'term': term, 'excerpt': sentence[:300],
                                 'status': 'Related study/project evidence exists; required production/professional use is not established',
                                 'uncertain_alternative': disjunction})
            lower = sentence.casefold()
            if re.search(r'\b(?:currently enrolled|current student|still enrolled)\b', lower):
                unknowns.append('Current-enrollment requirement needs checking; past graduation is not current enrollment.')
                uncertain_requirement = True
            if re.search(r'\b(?:no (?:prior |previous )?(?:professional |work )?experience|at most [012] years?|maximum [012] years?)\b', lower):
                unknowns.append('Prior-experience restriction needs checking against the recorded professional history.')
                uncertain_requirement = True
            years_match = re.search(r'\b(\d{1,2})(?:\s*[-–]\s*\d{1,2})?\s*\+?\s*years?\b', lower)
            if years_match:
                recorded = profile.get('candidate_summary', {}).get('professional_experience_years_approximate')
                if recorded is not None and int(years_match[1]) > recorded:
                    gaps.append({'term': 'experience duration', 'excerpt': sentence[:300],
                                 'status': 'Advert duration exceeds recorded total professional experience; verify what experience counts',
                                 'uncertain_alternative': disjunction})
                else:
                    unknowns.append('Required experience duration needs role-specific evidence; total software tenure does not prove specialist tenure.')
                    uncertain_requirement = True
        if not clauses:
            unknowns.append('Full job description is unavailable; title-only personalized fit is not assessed.')
        elif not any(kind in {'required', 'desirable', 'responsibility'} for _, kind in clauses):
            unknowns.append('Requirements/responsibilities could not be identified; lexical mentions alone are insufficient.')
        unknowns.append('Experimental lexical analysis may miss requirements or negation; verify the official advert.')
        substantive_gaps = [gap for gap in gaps if not gap['uncertain_alternative']]
        professional = sum(context == 'professional' for context in matched_groups.values())
        if not strengths:
            band = 'Stretch' if substantive_gaps and interests else 'Insufficient evidence'
        elif substantive_gaps:
            band = 'Stretch'
        elif len(matched_groups) >= 2 and professional and ai_context and not gaps and not uncertain_requirement:
            band = 'Review first'
        else:
            band = 'Possible fit'
        published = first_publication(job)
        published = published if published is not None and published <= now else None
        return {'job': copy.deepcopy(job), 'company': company, 'scorer_version': SCORER_VERSION,
                'profile_revision': profile['profile_revision'], 'profile_hash': self.profile_hash,
                'job_hash': fingerprint({'company': company, 'job': job}),
                'scope': 'out_of_scope' if conflicts else 'in_scope' if ai_context and
                    (matched_titles or preferences.get('ai_team_fallback', {}).get('enabled', False)) else 'review',
                'scope_conflicts': conflicts, 'priority': band, 'demonstrated_fit': strengths,
                'weaker_evidence': weaker, 'listed_skills': listed, 'career_direction': interests,
                'gaps': gaps, 'uncertainties': list(dict.fromkeys(unknowns)),
                'preferred_title_groups': matched_titles,
                'target_organization': {'id': org['id'], 'name': org['name']} if org else None,
                'parent_employer_candidate': {'id': parent['id'], 'name': parent['name']} if parent and not org else None,
                'ai_context_established': ai_context, 'evidence_group_count': len(matched_groups),
                'professional_group_count': professional,
                'original_publication': published.astimezone(timezone.utc).isoformat() if published else None}


def ordering(assessment):
    published = assessment['original_publication']
    date = datetime.fromisoformat(published).timestamp() if published else float('-inf')
    return (BANDS.index(assessment['priority']), -assessment['evidence_group_count'],
            -assessment['professional_group_count'], len(assessment['gaps']),
            -bool(assessment['target_organization']), -date, assessment['job_hash'])
