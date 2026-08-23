"""Who may read whose record — the care circle's decision, run locally.

The README's demo is "you sign in owning nothing and read someone else's
record". This is the rule that permits it, and the four it refuses. It needs no
database and no key: the decision is a value object and two properties, and
those are the real ones — `mirobody.user.care_circle.Membership` is the type the
repository returns, not a stand-in written for this script.

What the diagram in `docs/images/your-care-circle.svg` promises, and what the
schema enforces:

    health stays off until you allow it   ->  health_access DEFAULT 0
    your switch — off by default          ->  it is on YOUR OWN member row
    mutual — each controls their own      ->  B sharing with A says nothing
                                              about A sharing with B
    acceptance required to join           ->  status, and pending is not accepted

Run:  python examples/06_care_circle_rules.py
"""

from mirobody.user.care_circle import (
    ACCESS_EDIT,
    ACCESS_NAMES,
    ACCESS_NONE,
    ACCESS_VIEW,
    Membership,
)

print("What one member's switch grants the rest of their circle")
print("=" * 62)
print(f"  {'their health_access':<26}{'a read request':<18}a write request")
for level in (ACCESS_NONE, ACCESS_VIEW, ACCESS_EDIT):
    m = Membership(health_access=level)
    label = f"{level} ({ACCESS_NAMES[level]})"
    print(f"  {label:<26}{'allowed' if m.grants_view else 'refused':<18}"
          f"{'allowed' if m.grants_write else 'refused'}")

print("""
Read it down the first column. A member who has not touched the switch shares
nothing — joining a circle is not being seen. That is the default the column
`care_circle_members.health_access SMALLINT NOT NULL DEFAULT 0` sets, and no
other person's action can raise it: the switch lives on the row belonging to
the member whose record it describes.
""")

print("What `resolve_subject` does with that, against the database")
print("=" * 62)
print("""  resolve_subject(operator_id, subject_id, require_write=False)

    subject is None or myself      -> my own record, full access, always
    no accepted membership         -> raises CareCircleDenied
    membership grants less         -> raises CareCircleDenied
    membership grants enough       -> Subject(subject_id, access)

  Two properties worth knowing, both of which are the point:

  It RAISES. The check it replaced returned {"success": False, ...}, so eleven
  call sites each had to remember to read a key; forgetting one was a silent
  grant. An exception cannot be mistaken for success, and the server maps it to
  403 once, for every route.

  The grant is trimmed to the REQUEST, not to the relationship. Ask to read a
  record whose owner allowed read-write, and you get read. A path that only
  asked to read must not quietly carry the ability to write.
""")

print("Trying it for real")
print("=" * 62)
print("""  Needs a database, so it is not run here. With one:

    from mirobody.user.care_circle import resolve_subject, CareCircleDenied
    try:
        subject = await resolve_subject(my_id, their_id)
        print(subject.subject_id, subject.access)
    except CareCircleDenied as refused:
        print("no:", refused)

  `./deploy.sh` gives you one already populated: sign in as the account the
  server prints at startup and you are a member of the demo person's circle,
  with their switch at read.""")
