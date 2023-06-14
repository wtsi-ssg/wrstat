package basedirs

import (
	"os/user"
	"strconv"
)

type GroupCache map[uint32]string

func (g GroupCache) GroupName(gid uint32) string {
	groupName, ok := g[gid]
	if ok {
		return groupName
	}

	gidStr := strconv.FormatUint(uint64(gid), 10)

	group, err := user.LookupGroupId(gidStr)
	if err != nil {
		group.Name = gidStr
	}

	g[gid] = group.Name

	return group.Name
}

type UserCache map[uint32]string

func (u UserCache) UserName(uid uint32) string {
	userName, ok := u[uid]
	if ok {
		return userName
	}

	uidStr := strconv.FormatUint(uint64(uid), 10)

	user, err := user.LookupId(uidStr)
	if err != nil {
		user.Name = uidStr
	}

	u[uid] = user.Username

	return user.Username
}
