#ifndef OROCOS_RB_EXT_CORBA_HH
#define OROCOS_RB_EXT_CORBA_HH

#include <omniORB4/CORBA.h>

#include <exception>
#include "ControlTaskC.h"
#include "DataFlowC.h"
#include <iostream>
#include <string>
#include <stack>
#include <list>
#include <ruby.h>

using namespace std;

extern VALUE mCORBA;
extern VALUE corba_access;
extern VALUE eCORBA;
extern VALUE eComError;
namespace RTT
{
    class TaskContext;
    class PortInterface;
    namespace Corba
    {
        class ControlTaskServer;
    }
}

/**
 * This class locates and connects to a Corba ControlTask.
 * It can do that through an IOR or through the NameService.
 */
class CorbaAccess
{
    static CORBA::ORB_var orb;
    static CosNaming::NamingContext_var rootContext;

    RTT::TaskContext* m_task;
    RTT::Corba::ControlTaskServer* m_task_server;
    RTT::Corba::DataFlowInterface_var m_corba_dataflow;

    CorbaAccess(int argc, char* argv[] );
    ~CorbaAccess();
    static CorbaAccess* the_instance;

    // This counter is used to generate local port names that are unique
    int64_t port_id_counter;

public:
    static void init(int argc, char* argv[]);
    static void deinit();
    static CorbaAccess* instance() { return the_instance; }

    std::string getLocalPortName(VALUE remote_port);

    RTT::Corba::DataFlowInterface_ptr getDataFlowInterface() const;

    /** Reference a local port as a local manipulation interface to a given
     * remote port. The method adds the port on the local data flow interface,
     * and provides a good name for it (based on the remote port's name and the
     * remote port's task name)
     *
     * @returns the new port's name
     */
    void addPort(RTT::PortInterface* local_port);

    /** De-references a port that had been added by addPort
     */
    void removePort(RTT::PortInterface* local_port);

    /** Returns the list of tasks that are registered on the name service. Some
     * of them can be invalid references, as for instance a process crashed and
     * did not clean up its references
     */
    std::list<std::string> knownTasks();

    /** Returns a ControlTask reference to a remote control task. The reference
     * is assured to be valid.
     */
    RTT::Corba::ControlTask_ptr findByName(std::string const& name);

    /** Unbinds a particular control task on the name server
     */
    void unbind(std::string const& name);
};

#define CORBA_EXCEPTION_HANDLERS \
    catch(CORBA::COMM_FAILURE&) { rb_raise(eComError, ""); } \
    catch(CORBA::TRANSIENT&) { rb_raise(eComError, ""); } \
    catch(CORBA::Exception& e) { rb_raise(eCORBA, "unspecified error in the CORBA layer: %s", typeid(e).name()); }

#endif

